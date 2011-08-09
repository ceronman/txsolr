clean:
	find . -name \*pyc -exec rm {} \;
	rm -rf doc
	rm -rf _trial_temp

check:
	trial txsolr

info:
	@bzr info
	@echo
	@echo "Revision:"
	@bzr revno
	@echo
	@echo "Lines of application code:"
	@find txsolr -name \*py | grep -v test_ | xargs cat | wc -l
	@echo
	@echo "Lines of test code:"
	@find txsolr -name \*py | grep test_ | xargs cat | wc -l

lint:
	@pep8 --repeat $(shell find . -name \*py)
	@pyflakes $(shell find . -name \*py)

api:
	pydoctor --project-name txSolr \
             --make-html \
             --html-output doc \
	         --add-package txsolr

release:
	python setup.py sdist
