package main

import "fmt"

func main() {
	cache := NewLRUCache(3)

	cache.Put("A", "Dadang")
	cache.Put("B", "Dudung")
	cache.Put("C", "Dedeng")

	fmt.Println(cache.Get("A"))

	cache.Put("D", "Dodong")

	fmt.Println(cache.Get("B"))

	fmt.Println(cache.Get("C"))

	fmt.Println(cache.Get("A"))

	fmt.Println(cache.Get("D"))

	fmt.Println("Length:", cache.GetSize())
}
