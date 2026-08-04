module dosloader

go 1.26.5

require (
	github.com/spf13/cobra v1.10.2
	golang.org/x/net v0.56.0
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	golang.org/x/text v0.38.0 // indirect
)

replace github.com/inconshreveable/mousetrap => ./third_party/mousetrap

replace github.com/spf13/pflag => github.com/spf13/pflag v1.0.10

replace gopkg.in/check.v1 => ./third_party/check
