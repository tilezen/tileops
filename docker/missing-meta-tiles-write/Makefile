TAG ?= latest
IMAGE ?= tilezen/missing-meta-tiles-write
AWS_DEFAULT_REGION ?= us-east-1
ifndef REGISTRY
$(error REGISTRY variable is not set)
endif
ifndef GOPATH
$(error GOPATH variable is not set)
endif

image: binary
	docker build -t $(IMAGE) .

push: .ecr-logged-in
	docker tag $(IMAGE):$(TAG) $(REGISTRY)/$(IMAGE):$(TAG)
	docker push $(REGISTRY)/$(IMAGE):$(TAG)

.ecr-logged-in:
	`aws ecr get-login --no-include-email --region $(AWS_DEFAULT_REGION) | tr -d '\r'`
	touch .ecr-logged-in

binary: tz-missing-meta-tiles-write

tz-missing-meta-tiles-write:
	CGO_ENABLED=0 GOOS=linux go build -a -tags netgo -ldflags '-w' tzops/go/cmd/tz-missing-meta-tiles-write

create-job-definition: $(GOPATH)/bin/tz-batch-create-job-definition
	tz-batch-create-job-definition -yaml job-definition.yaml

$(GOPATH)/bin/tz-batch-create-job-definition:
	go install tzops/go/cmd/tz-batch-create-job-definition

clean:
	rm -f tz-missing-meta-tiles-write .ecr-logged-in

.PHONY: image push binary clean create-job-definition
