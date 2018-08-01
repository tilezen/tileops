TAG ?= latest
IMAGE ?= tilezen/missing-meta-tiles-write
AWS_DEFAULT_REGION ?= us-east-1
ifndef REGISTRY
$(error REGISTRY variable is not set)
endif
BINARY = $(shell which tz-missing-meta-tiles-write)
ifeq ($(BINARY),)
$(error missing tz-missing-meta-tiles-write command)
endif

image: binary
	docker build -t $(IMAGE) .

binary: tz-missing-meta-tiles-write

tz-missing-meta-tiles-write: $(BINARY)
	cp $< $@

push: .ecr-logged-in
	docker tag $(IMAGE):$(TAG) $(REGISTRY)/$(IMAGE):$(TAG)
	docker push $(REGISTRY)/$(IMAGE):$(TAG)

.ecr-logged-in:
	`aws ecr get-login --no-include-email --region $(AWS_DEFAULT_REGION) | tr -d '\r'`
	touch .ecr-logged-in

clean:
	rm -f tz-missing-meta-tiles-write .ecr-logged-in

.PHONY: image push binary clean
