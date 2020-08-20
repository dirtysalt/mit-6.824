run-worker:
	go run mrworker.go wc.so
run-master:
	go run mrmaster.go pg-*.txt
build-mr:
	go build -buildmode=plugin ../mrapps/wc.go
clean:
	rm -rf mr-out* mr-swap mr-temp
killall:
	ps aux | grep "mrworker" | awk '{print $$2}' | xargs kill
	ps aux | grep "mrworker" | awk '{print $$2}' | xargs kill
