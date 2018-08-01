package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"text/template"
	"time"
	"tzops/go/pkg/cmd"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/iam"
)

type AssumeRolePolicyDocument struct {
	Version   string                      `json:"Version"`
	Statement []AssumeRolePolicyStatement `json:"Statement"`
}

type AssumeRolePolicyStatement struct {
	Effect    string                    `json:"Effect"`
	Principal AssumeRolePolicyPrincipal `json:"Principal"`
	Action    string                    `json:"Action"`
}

type AssumeRolePolicyPrincipal struct {
	Service string `json:"Service"`
}

func NewAssumeRolePolicyDocument(service string) AssumeRolePolicyDocument {
	return AssumeRolePolicyDocument{
		Version: "2012-10-17",
		Statement: []AssumeRolePolicyStatement{
			AssumeRolePolicyStatement{
				Effect: "Allow",
				Action: "sts:AssumeRole",
				Principal: AssumeRolePolicyPrincipal{
					Service: service,
				},
			},
		},
	}
}

type InlinePolicyDocument struct {
	Version   string                  `json:"Version"`
	Statement []InlinePolicyStatement `json:"Statement"`
}

type InlinePolicyStatement struct {
	Effect   string               `json:"Effect"`
	Action   []string             `json:"Action"`
	Resource InlinePolicyResource `json:"Resource"`
}

type InlinePolicyResource struct {
	single   string
	multiple []string
}

func (ipr InlinePolicyResource) MarshalJSON() ([]byte, error) {
	if ipr.single != "" {
		return json.Marshal(ipr.single)
	} else if len(ipr.multiple) > 0 {
		return json.Marshal(ipr.multiple)
	} else {
		return nil, errors.New("empty inline policy resource")
	}
}

func NewSingleInlinePolicyResource(resource string) InlinePolicyResource {
	return InlinePolicyResource{single: resource, multiple: nil}
}

func NewMultipleInlinePolicyResource(resources []string) InlinePolicyResource {
	return InlinePolicyResource{single: "", multiple: resources}
}

func NewInlinePolicyDocument(statements []InlinePolicyStatement) InlinePolicyDocument {
	return InlinePolicyDocument{
		Version:   "2012-10-17",
		Statement: statements,
	}
}

func NewSingleStatementInlinePolicyDocument(actions []string, resource InlinePolicyResource) InlinePolicyDocument {
	return NewInlinePolicyDocument(
		[]InlinePolicyStatement{NewInlinePolicyStatement(actions, resource)},
	)
}

func NewInlinePolicyStatement(actions []string, resource InlinePolicyResource) InlinePolicyStatement {
	return InlinePolicyStatement{
		Effect:   "Allow",
		Action:   actions,
		Resource: resource,
	}
}

func createOrchestrationProfile(svc *iam.IAM, profileName string) (*iam.InstanceProfile, error) {
	assumeRolePolicyDocument := NewAssumeRolePolicyDocument("ec2.amazonaws.com")
	b, err := json.Marshal(&assumeRolePolicyDocument)
	if err != nil {
		return nil, err
	}
	cipo, err := svc.CreateInstanceProfile(&iam.CreateInstanceProfileInput{
		InstanceProfileName: &profileName,
		Path:                aws.String("/"),
	})
	if err != nil {
		return nil, err
	}
	_, err = svc.CreateRole(&iam.CreateRoleInput{
		RoleName: &profileName,
		Path:     aws.String("/"),
		AssumeRolePolicyDocument: aws.String(string(b)),
	})
	if err != nil {
		return nil, err
	}
	_, err = svc.AddRoleToInstanceProfile(&iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: &profileName,
		RoleName:            &profileName,
	})
	if err != nil {
		return nil, err
	}

	for _, policy := range []string{
		"AmazonRDSFullAccess",
		"AmazonEC2ContainerRegistryFullAccess",
		"AWSBatchFullAccess",
	} {
		policyArn := "arn:aws:iam::aws:policy/" + policy
		_, err = svc.AttachRolePolicy(&iam.AttachRolePolicyInput{
			RoleName:  &profileName,
			PolicyArn: &policyArn,
		})
		if err != nil {
			return nil, err
		}
	}

	ec2InlinePolicy := NewSingleStatementInlinePolicyDocument(
		[]string{
			"ec2:AuthorizeSecurityGroupIngress",
			"ec2:DescribeInstances",
			"ec2:TerminateInstances",
			"ec2:CreateKeyPair",
			"ec2:CreateTags",
			"ec2:RunInstances",
			"ec2:DescribeSecurityGroups",
			"ec2:DescribeImages",
			"ec2:CreateSecurityGroup",
			"ec2:DeleteSecurityGroup",
			"ec2:DescribeSubnets",
			"ec2:DeleteKeyPair",
			"ec2:DescribeInstanceStatus",
		},
		NewSingleInlinePolicyResource("*"),
	)
	iamInlinePolicy := NewSingleStatementInlinePolicyDocument(
		[]string{
			"iam:ListPolicies",
			"iam:CreatePolicy",
			"iam:GetRole",
			"iam:CreateRole",
			"iam:AttachRolePolicy",
			"iam:PassRole",
		},
		NewSingleInlinePolicyResource("*"),
	)
	s3InlinePolicy := NewInlinePolicyDocument(
		[]InlinePolicyStatement{
			NewInlinePolicyStatement(
				[]string{
					"s3:ListBucket",
					"s3:DeleteObject",
				},
				NewMultipleInlinePolicyResource([]string{
					"arn:aws:s3:::sc-snapzen-missing-tiles-us-east-2",
					"arn:aws:s3:::sc-snapzen-missing-tiles-us-east-2/*",
				}),
			),
			NewInlinePolicyStatement(
				[]string{
					"s3:GetObject",
				},
				NewSingleInlinePolicyResource("arn:aws:s3:::sc-snapzen-tile-assets-us-east-2/*"),
			),
		},
	)

	for _, namedInlinePolicy := range []struct {
		name   string
		policy *InlinePolicyDocument
	}{
		{"AllowEC2", &ec2InlinePolicy},
		{"AllowIAM", &iamInlinePolicy},
		{"AllowS3", &s3InlinePolicy},
	} {
		b, err := json.Marshal(namedInlinePolicy.policy)
		if err != nil {
			return nil, err
		}
		doc := string(b)
		_, err = svc.PutRolePolicy(&iam.PutRolePolicyInput{
			RoleName:       &profileName,
			PolicyName:     &namedInlinePolicy.name,
			PolicyDocument: &doc,
		})
		if err != nil {
			return nil, err
		}
	}

	return cipo.InstanceProfile, nil
}

func checkStringArg(s string, msg string) {
	if s == "" {
		fmt.Fprintf(os.Stderr, "%s\n", msg)
		cmd.DieWithUsage()
	}
}

func main() {
	var (
		dateYYYYMMDD        string
		profileName         string
		shouldCreateProfile bool
		keyName             string
		tmplPath            string
		region              string
		bucketAssets        string
		bucketRawr          string
		bucketMeta          string
		bucketMissing       string
		assetsRoleARN       string
		dbPassword          string
	)
	flag.StringVar(&dateYYYYMMDD, "date", "", "yyyymmdd")
	flag.StringVar(&profileName, "profile", "",
		"profile name for orchestration ec2 instance")
	flag.StringVar(&keyName, "ssh-key", "", "name of ssh key pair to use")
	flag.StringVar(&region, "region", "us-east-2", "region")
	flag.BoolVar(&shouldCreateProfile, "create-profile", false,
		"whether to create the profile")
	flag.StringVar(&tmplPath, "template", "", "path to user data template")
	flag.StringVar(&bucketAssets, "bucket-assets", "", "tile assets s3 bucket")
	flag.StringVar(&bucketRawr, "bucket-rawr", "", "rawr s3 bucket")
	flag.StringVar(&bucketMeta, "bucket-meta", "", "meta s3 bucket")
	flag.StringVar(&bucketMissing, "bucket-missing", "", "missing tiles s3 bucket")
	flag.StringVar(&assetsRoleARN, "tile-assets-role-arn", "", "tile assets role arn")
	flag.StringVar(&dbPassword, "dbpassword", "", "database password")
	flag.Parse()
	checkStringArg(dateYYYYMMDD, "date required")
	checkStringArg(profileName, "profile required")
	checkStringArg(keyName, "ssh-key required")
	checkStringArg(tmplPath, "template required")
	checkStringArg(bucketAssets, "bucket-assets required")
	checkStringArg(bucketRawr, "bucket-rawr required")
	checkStringArg(bucketMeta, "bucket-meta required")
	checkStringArg(bucketMissing, "bucket-missing required")
	checkStringArg(assetsRoleARN, "tile-assets-role-arn required")
	checkStringArg(dbPassword, "dbpassword required")
	sess := session.Must(session.NewSession(&aws.Config{
		Region: &region,
	}))

	tzDate, err := newTZDateFromYYYYMMDD(dateYYYYMMDD)
	if err != nil {
		log.Fatalf("error parsing date %s: %s\n", dateYYYYMMDD, err)
	}

	iamSvc := iam.New(sess)
	var profile *iam.InstanceProfile
	gipi, err := iamSvc.GetInstanceProfile(&iam.GetInstanceProfileInput{
		InstanceProfileName: &profileName,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NoSuchEntity" {
			if shouldCreateProfile {
				profile, err = createOrchestrationProfile(iamSvc, profileName)
				if err != nil {
					log.Fatalf("error: failed to create profile %s: %s\n", profileName, err)
				}
			} else {
				log.Fatalf("error: unknown profile %s\n", profileName)
			}
		} else {
			log.Fatalf("error getting profile %s: %s\n", profileName, err)
		}
		err = iamSvc.WaitUntilInstanceProfileExists(&iam.GetInstanceProfileInput{
			InstanceProfileName: &profileName,
		})
		if err != nil {
			log.Fatalf("error waiting for creating instance profile %s: %s\n", profileName, err)
		}
		// not sure why, but even after waiting, the ec2 instance creation
		// fails with an invalid profile arn error
		// sleeping a little bit of time fixes it :(
		time.Sleep(10 * time.Second)
	} else {
		if gipi.InstanceProfile == nil {
			log.Fatalln("error profile not found in response")
		}
		profile = gipi.InstanceProfile
	}

	userDataGen, err := newUserDataGenerator(tmplPath)
	if err != nil {
		log.Fatalf("error creating user data generator: %s\n", err)
	}

	templateData := TemplateData{
		AWSConfig: AWSConfig{
			Buckets: BucketsConfig{
				Assets:  bucketAssets,
				Rawr:    bucketRawr,
				Meta:    bucketMeta,
				Missing: bucketMissing,
			},
			AssetsRoleARN: assetsRoleARN,
			DBPassword:    dbPassword,
			Region:        region,
		},
		Date: tzDate,
	}

	rawUserData, err := userDataGen.Generate(&templateData)
	if err != nil {
		log.Fatalf("error generating user data: %s\n", err)
	}
	userData := base64Encode(rawUserData)

	ec2Svc := ec2.New(sess)
	rio, err := ec2Svc.RunInstances(&ec2.RunInstancesInput{
		KeyName:  &keyName,
		MaxCount: aws.Int64(1),
		MinCount: aws.Int64(1),
		// TODO configurable?
		InstanceType: aws.String(ec2.InstanceTypeT2Micro),
		// TODO configurable?
		ImageId: aws.String("ami-06615263"),
		// TODO manage this somehow? should we create it as part of the script
		// too? and whitelist nyc?
		SecurityGroupIds: []*string{aws.String("sg-1d86f577")},
		IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
			Arn: profile.Arn,
		},
		UserData: &userData,
	})
	_ = profile
	if err != nil {
		log.Fatalf("error creating ec2 instance: %s\n", err)
	}
	reservationId := rio.ReservationId
	if len(rio.Instances) != 1 {
		log.Fatalln("error creating ec2 instance")
	}
	fmt.Printf("reservation id: %s\n", *reservationId)
	instance := rio.Instances[0]
	fmt.Printf("instance id: %s\n", *instance.InstanceId)
}

type userDataGenerator struct {
	tmpl *template.Template
}

func newUserDataGenerator(tmplPath string) (*userDataGenerator, error) {
	b, err := ioutil.ReadFile(tmplPath)
	if err != nil {
		return nil, err
	}
	tmpl, err := template.New("userdata").Parse(string(b))
	if err != nil {
		return nil, err
	}
	return &userDataGenerator{
		tmpl: tmpl,
	}, nil
}

func (g *userDataGenerator) Generate(tmplState interface{}) (string, error) {
	var buf bytes.Buffer
	err := g.tmpl.Execute(&buf, tmplState)
	if err != nil {
		return "", err
	}
	res := buf.String()
	return res, nil
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

type AWSConfig struct {
	Region        string
	AssetsRoleARN string
	DBPassword    string
	Buckets       BucketsConfig
}

type BucketsConfig struct {
	Assets  string
	Rawr    string
	Meta    string
	Missing string
}

type TemplateData struct {
	AWSConfig
	Date TZDate
}

type TZDate time.Time

const timeYYYYMMDD = "20060102"
const timeYYYY_DASH_MM_DASH_DD = "2006-01-02"
const timeYYMMDD = "060102"

func newTZDateFromYYYYMMDD(s string) (TZDate, error) {
	var t time.Time
	var err error
	t, err = time.Parse(timeYYYYMMDD, s)
	if err != nil {
		return TZDate(t), err
	}
	return TZDate(t), nil
}

func (tzDate TZDate) YYYY_DASH_MM_DASH_DD() string {
	return time.Time(tzDate).Format(timeYYYY_DASH_MM_DASH_DD)
}

func (tzDate TZDate) YYMMDD() string {
	return time.Time(tzDate).Format(timeYYMMDD)
}
