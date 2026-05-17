package tape

import (
	"context"

	"google.golang.org/grpc/credentials"
)

// staticToken — fixed Bearer token, attached on every RPC. Used when the caller
// supplies an Options.IDToken.
type staticToken struct{ token string }

func (s staticToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + s.token}, nil
}
func (s staticToken) RequireTransportSecurity() bool { return true }

// newIDTokenSource — returns a PerRPCCredentials that mints a Google OIDC ID
// token for `audience` (via google.golang.org/api/idtoken). If the idtoken
// package isn't available (not built in) or ADC isn't reachable, returns nil so
// the caller can proceed without auth (fine for TLS-without-IAM endpoints).
//
// This is built without a hard dependency on idtoken; if you want the auto-auth
// behaviour, add `google.golang.org/api/idtoken` to your go.mod and replace
// this body with the two-line version below. (Kept this way so the SDK builds
// without pulling the full google-api-go-client tree.)
//
//	import "google.golang.org/api/idtoken"
//	ts, err := idtoken.NewTokenSource(context.Background(), audience)
//	if err != nil { return nil, err }
//	return oauth.TokenSource{TokenSource: ts}, nil
func newIDTokenSource(audience string) (credentials.PerRPCCredentials, error) {
	_ = audience
	return nil, nil
}

var _ = credentials.PerRPCCredentials(staticToken{})
