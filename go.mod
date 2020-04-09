module github.com/ezbuy/disque-go

go 1.14

require (
	github.com/garyburd/redigo v0.0.0-20151029235527-6ece6e0a09f2
	github.com/golang/glog v0.0.0-20141105023935-44145f04b68c // indirect
	github.com/stretchr/testify v0.0.0-20150218221846-e4ec8152c15f
	github.com/youtube/vitess v2.1.1+incompatible
	golang.org/x/net v0.0.0-20150429055707-a8c61998a557
)

replace github.com/youtube/vitess => ./vendor/github.com/youtube/vitess
