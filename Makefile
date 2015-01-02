build: clean
	docker build -t="alangpierce/appengine-python3" .

# It looks like .dockerignore currently isn't expressive enough to do recursive
# traversals, so just delete temporary files that shouldn't end up in the image.
clean:
	find . \( -name '*.pyc' -o -name '__pycache__' -o -name '.DS_Store' \) -delete
