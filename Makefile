_output=_output

binaries=resource-scheduler

.PHONY: $(binaries)
$(binaries):
	go build -o $(_output)/$@ cmd/$@/main.go

.PHONY: clean
clean:
	rm -rf $(_output)
