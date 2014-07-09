.PHONY: dist

all: debug

dist:
	python setup.py sdist
	python setup.py bdist_wheel

clean:
	rm -rf build dist pygear.so
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

debug: clean
	python setup.py build_ext -g --inplace

install:
	python setup.py install

test:
	tox
