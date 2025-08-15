all:
	npx @marp-team/marp-cli@latest --allow-local-files --html --pdf Presentation.md -o Presentation.pdf

thumbnail:
	npx @marp-team/marp-cli@latest --allow-local-files --html --image=png Presentation.md

watch:
	zathura Presentation.pdf &
	npx @marp-team/marp-cli@latest -w --allow-local-files --html --pdf Presentation.md -o Presentation.pdf

present:
	npx @marp-team/marp-cli@latest -s . --allow-local-files --html

clean:
	rm -rf *.pdf
