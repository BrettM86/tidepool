package identity

import (
	"net/http"
	"strings"
)

// DIDWebHandler serves GET /.well-known/did.json — the DID document for the
// bridge's own did:web service identity. The bridge identifies as
// did:web:<BRIDGE_HOSTNAME> when no BRIDGE_SERVICE_DID is provisioned, and
// that DID appears as `hostedBy` in every bridged community.profile record;
// consumers verifying the claim (the Coves AppView's bidirectional check
// above all) resolve it right here and require `id` to equal the DID and
// `alsoKnownAs` to contain at://<hostname>. Without this document the
// service DID is unresolvable and every hostedBy verification fails closed.
//
// Only the did:web method needs the endpoint: when the operator provisions
// a BRIDGE_SERVICE_DID of any other method (did:plc), the document lives in
// that method's directory instead and this handler answers 404.
func DIDWebHandler(serviceDID, hostname string) http.HandlerFunc {
	hostname = strings.ToLower(hostname)
	serves := serviceDID == "did:web:"+hostname

	return func(w http.ResponseWriter, r *http.Request) {
		if !serves {
			http.Error(w, "no did:web identity on this host", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"@context":    []string{"https://www.w3.org/ns/did/v1"},
			"id":          serviceDID,
			"alsoKnownAs": []string{"at://" + hostname},
		})
	}
}
