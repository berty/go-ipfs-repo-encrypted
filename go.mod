module berty.tech/go-ipfs-repo-encrypted

go 1.16

require (
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-sql v0.3.0
	github.com/ipfs/go-filestore v1.2.0
	github.com/ipfs/go-ipfs v0.11.0
	github.com/ipfs/go-ipfs-config v0.19.0
	github.com/ipfs/go-ipfs-keystore v0.0.2
	github.com/libp2p/go-libp2p-core v0.13.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/mutecomm/go-sqlcipher/v4 v4.4.2
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
)

replace github.com/mutecomm/go-sqlcipher/v4 => github.com/berty/go-sqlcipher/v4 v4.4.3-0.20220810151512-74ea78235b48 // plaintext header support
