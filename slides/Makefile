all:
	npx marp --allow-local-files --html --pdf Presentation.md -o Presentation.pdf

thumbnail:
	npx marp --allow-local-files --html --image=png Presentation.md

watch:
	zathura Presentation.pdf &
	npx marp -w --allow-local-files --html --pdf Presentation.md -o Presentation.pdf

present:
	npx marp -s . --allow-local-files --html

clean:
	rm -rf *.pdf
