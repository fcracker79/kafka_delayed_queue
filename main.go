package main

import "reactor"

func main() {
	reactor.StartReactor([]string{"localhost:32181"}, "AClientID")
}
