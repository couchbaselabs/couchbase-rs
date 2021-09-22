update:
	GOOS=darwin GOARCH=amd64 go build -o generator  .
	mv generator ./macos/testgenerator
	GOOS=linux GOARCH=amd64 go build -o generator  .
	mv generator ./linux/testgenerator
	GOOS=windows GOARCH=amd64 go build -o generator.exe  .
	mv generator.exe ./windows/testgenerator.exe

.PHONY: update
