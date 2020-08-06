package main

import (
	"flag"
	"fmt"
	"github.com/tilezen/tileops.git/go/pkg/cmd"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"

	"gopkg.in/yaml.v2"
)

type jobDefinitionConfig struct {
	Name          string            `yaml:"name"`
	JobRoleArn    string            `yaml:"job-role-arn"`
	Image         string            `yaml:"image"`
	Command       []string          `yaml:"command"`
	Environment   map[string]string `yaml:"environment"`
	Memory        uint              `yaml:"memory"`
	Vcpus         uint              `yaml:"vcpus"`
	RetryAttempts uint              `yaml:"retry-attempts"`
}
type jobDefinitionsConfig struct {
	Definitions []jobDefinitionConfig `yaml:"job-definitions"`
	Region      string                `yaml:"region"`
}

func main() {
	var yamlPath string

	flag.StringVar(&yamlPath, "yaml", "", "path to yaml file")
	flag.Parse()

	if yamlPath == "" {
		cmd.DieWithUsage()
	}

	yamlFile, err := os.Open(yamlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid yaml path: %s\n", yamlPath)
		cmd.DieWithUsage()
	}
	yamlDec := yaml.NewDecoder(yamlFile)
	var jobsYaml jobDefinitionsConfig
	err = yamlDec.Decode(&jobsYaml)
	if err != nil {
		panic(err)
	}
	err = yamlFile.Close()
	if err != nil {
		panic(err)
	}
	region := jobsYaml.Region
	if region == "" {
		region = "us-east-1"
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:     &region,
		MaxRetries: aws.Int(3),
	}))
	svc := batch.New(sess)

	for _, jobDef := range jobsYaml.Definitions {
		command := make([]*string, len(jobDef.Command))
		for i := 0; i < len(jobDef.Command); i++ {
			command[i] = &jobDef.Command[i]
		}
		environment := make([]*batch.KeyValuePair, 0, len(jobDef.Environment))
		for k, v := range jobDef.Environment {
			environment = append(environment, &batch.KeyValuePair{
				Name:  aws.String(k),
				Value: aws.String(v),
			})
		}

		input := batch.RegisterJobDefinitionInput{
			JobDefinitionName: &jobDef.Name,
			Type:              aws.String(batch.JobDefinitionTypeContainer),
			ContainerProperties: &batch.ContainerProperties{
				Command:     command,
				Environment: environment,
				Image:       &jobDef.Image,
				JobRoleArn:  &jobDef.JobRoleArn,
				Memory:      aws.Int64(int64(jobDef.Memory)),
				Vcpus:       aws.Int64(int64(jobDef.Vcpus)),
				ReadonlyRootFilesystem: aws.Bool(true),
			},
			RetryStrategy: &batch.RetryStrategy{
				Attempts: aws.Int64(int64(jobDef.RetryAttempts)),
			},
		}
		output, err := svc.RegisterJobDefinition(&input)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s created: %s\n", *output.JobDefinitionName, *output.JobDefinitionArn)
	}

}
