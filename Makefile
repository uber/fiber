.PHONY: docker docs

docker:
	docker build . -t fiber-test

docker-pytorch:
	docker build . -f fiber-pytorch.docker -t fiber-pytorch

test: docker
	./test.sh

ltest:
	./test_local.sh

ktest:
	./test_kubernetes.sh

cov:
	pytest --cov-report html --cov-report annotate --cov=fiber tests

lint:
	flake8 fiber

style:
	isort fiber/*.py

docs:
	cd mkdocs && pydocmd build -d ../docs
	cp docs/img/favicon.ico docs/favicon.ico

serve:
	cd mkdocs && pydocmd serve -a 0.0.0.0:8000 --livereload
