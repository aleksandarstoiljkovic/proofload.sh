// Package backends registers every built-in provisioner backend by blank import.
// Engine binaries import this one package so they can provision on any supported
// backend (kubernetes, compose) via --provision, without each main needing to
// know the full backend list. The external backend needs no registration (it is
// the default "connect only" path when no provisioner is selected).
package backends

import (
	_ "github.com/proofload/proofload/core/provision/compose"
	_ "github.com/proofload/proofload/core/provision/k8s"
)
