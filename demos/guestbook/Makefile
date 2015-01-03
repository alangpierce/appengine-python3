serve: clean
	gcloud preview app run .

deploy: clean
	gcloud preview app deploy .

clean:
	find . \( -name '*.pyc' -o -name '__pycache__' -o -name '.DS_Store' \) -delete
