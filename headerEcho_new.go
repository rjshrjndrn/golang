// This is a simple webserver runs on port 4000 and will
// print all headers, path, and HTTPVersion version
// of the particular request.
//
// Docker image is published
// docker run --rm -p 4000:4000 rjshrjndrn/headerecho
//
// example: go run headerEcho_new.go
//
// curl localhost:4000/dummy/path -Hhello:world
// 		"User-Agent": ["curl/7.67.0"]
// 		"Accept": ["*/*"]
// 		"Hello": ["world"]
// 		"HTTPVersion": "HTTP/1.1"
// 		"RequestPath": "/dummy/path"

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func helloError(res http.ResponseWriter, r *http.Request) {
	res.WriteHeader(500)
	res.Write([]byte("Boom!"))
}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%s\n", reqBody)
		fmt.Fprintf(w, "Body: %s\n", reqBody)
	default:
		for k, v := range r.Header {
			fmt.Fprintf(w, "%s: %s\n", k, v)
		}
	}
	fmt.Fprintf(w, "HTTPVersion: %s\n", r.Proto)
	fmt.Fprintf(w, "RequestPath: %s\n", r.URL.Path)
	fmt.Fprintf(w, "Version: v3")
}

func main() {
	http.HandleFunc("/", helloWorld)
	http.HandleFunc("/error", helloError)
	http.ListenAndServe(":4000", nil)
}
