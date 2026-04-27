.PHONY: download web serve deploy clean

download:
	./fetch_payments.py

web: payments.csv
	./build_web.py

serve: web
	@echo "→ http://localhost:8000"
	python3 -m http.server -d web 8000

deploy: web
	npx wrangler pages deploy web --project-name latwogang

clean:
	rm -rf web/data
