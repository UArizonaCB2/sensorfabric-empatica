# This Makefile is only for testing.

debug-nodate: *.py
	python3 Controller.py -d ../Together-YA/empatica_raw/participant_data -p s3://togetherya-test/empatica/ -db togetherya-test

debug-date: *.py
	python3 Controller.py -d ../Together-YA/empatica_raw/participant_data -p s3://togetherya-test/empatica/ -db togetherya-test -s 2024-01-01
