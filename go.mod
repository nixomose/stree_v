module github.com/nixomose/stree_v

go 1.18

// replace github.com/nixomose/blockdevicelib => ../blockdevicelib

// replace github.com/nixomose/stree_v => ../stree_v

// replace github.com/nixomose/zosbd2goclient => ../zosbd2goclient

// replace github.com/nixomose/nixomosegotools => ../nixomosegotools

require (
	github.com/ncw/directio v1.0.5
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
)

require (
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/nixomose/nixomosegotools v0.0.0-20220601021307-4fef638de118
	github.com/nixomose/zosbd2goclient v0.0.0-20220601013206-758ca3d1fa3d
	github.com/spf13/cobra v1.4.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
)
