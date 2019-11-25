module github.com/Financial-Times/concept-exporter

go 1.13

require (
	github.com/Financial-Times/annotations-rw-neo4j/v3 v3.2.0
	github.com/Financial-Times/api-endpoint v0.0.0-20170713111258-802a63542ff0
	github.com/Financial-Times/base-ft-rw-app-go v0.0.0-20171010162315-74eab27b0c6d
	github.com/Financial-Times/concepts-rw-neo4j v1.23.2
	github.com/Financial-Times/content-rw-neo4j v1.0.3-0.20171011115956-641ce08b0417
	github.com/Financial-Times/financial-instruments-rw-neo4j v0.0.4-0.20170725130533-fedc405769c8
	github.com/Financial-Times/go-fthealth v0.0.0-20181009114238-ca83ad65381f
	github.com/Financial-Times/go-logger v0.0.0-20180323124113-febee6537e90
	github.com/Financial-Times/go-logger/v2 v2.0.1
	github.com/Financial-Times/http-handlers-go v0.0.0-20180517120644-2c20324ab887
	github.com/Financial-Times/http-handlers-go/v2 v2.1.0
	github.com/Financial-Times/neo-model-utils-go v0.0.0-20180712095719-aea1e95c8305
	github.com/Financial-Times/neo-utils-go v0.0.0-20181119150836-7fc6c3f7b78f
	github.com/Financial-Times/organisations-rw-neo4j v0.0.0-20170901151145-1dce3ba256e3
	github.com/Financial-Times/service-status-go v0.0.0-20160323111542-3f5199736a3d
	github.com/Financial-Times/transactionid-utils-go v0.2.0
	github.com/Financial-Times/up-rw-app-api-go v0.0.0-20170710125828-d9d93a1f6895
	github.com/cyberdelia/go-metrics-graphite v0.0.0-20161219230853-39f87cc3b432
	github.com/google/uuid v1.1.1 // indirect
	github.com/gorilla/context v1.1.1
	github.com/gorilla/handlers v1.4.0
	github.com/gorilla/mux v1.7.3
	github.com/hashicorp/go-version v1.2.0
	github.com/jawher/mow.cli v1.1.0
	github.com/jmcvetta/neoism v1.3.2-0.20170306104137-4674154f870d
	github.com/jmcvetta/randutil v0.0.0-20150817122601-2bb1b664bcff
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0
	github.com/kr/text v0.1.0
	github.com/pborman/uuid v1.2.0
	github.com/pkg/errors v0.8.1-0.20171018195549-f15c970de5b7
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
	github.com/sethgrid/pester v0.0.0-20190127155807-68a33a018ad0
	github.com/sirupsen/logrus v1.4.2
	github.com/spaolacci/murmur3 v0.0.0-20170806121338-4ec5a0f56d4f
	github.com/stretchr/testify v1.3.0
	github.com/ugorji/go v0.0.0-20170620104852-5efa3251c7f7
	go4.org v0.0.0-20191010144846-132d2879e1e9
	golang.org/x/crypto v0.0.0-20191119213627-4f8c1d86b1ba
	golang.org/x/net v0.0.0-20191119073136-fc4aabc6c914
	golang.org/x/sys v0.0.0-20191120155948-bd437916bb0e
	gopkg.in/jmcvetta/napping.v3 v3.2.0
	gopkg.in/stretchr/testify.v1 v1.4.0
	gopkg.in/yaml.v2 v2.2.7
)

replace gopkg.in/stretchr/testify.v1 => github.com/stretchr/testify v1.4.0
