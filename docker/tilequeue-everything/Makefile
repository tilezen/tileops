TAG ?= latest
IMAGE ?= tilezen/rawr-batch
AWS_DEFAULT_REGION ?= us-east-1
ifndef REGISTRY
$(error REGISTRY variable is not set)
endif

REPOS = vector-datasource tilequeue raw_tiles

image: $(REPOS)
	docker build -t $(IMAGE) .

$(REPOS):
	[ -d ../../../$@ ] && cp -a ../../../$@ . && (cd $@ && rm -rf .git integration-test docs)

login: .ecr-logged-in

.ecr-logged-in:
	`aws ecr get-login --no-include-email --region $(AWS_DEFAULT_REGION) | tr -d '\r'`
	touch .ecr-logged-in

push: .ecr-logged-in
	docker tag $(IMAGE):$(TAG) $(REGISTRY)/$(IMAGE):$(TAG)
	docker push $(REGISTRY)/$(IMAGE):$(TAG)

clean:
	rm -rf .ecr-logged-in $(REPOS)

.PHONY: image login push clean
