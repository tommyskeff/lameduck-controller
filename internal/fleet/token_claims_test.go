package fleet

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"
)

func jwt(payload string) string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".c2ln"
}

func TestUnverifiedExpiry(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tests := []struct {
		name  string
		token string
		ok    bool
	}{
		{name: "audience list", token: jwt(`{"exp":1000060,"aud":["other","lameduck"]}`), ok: true},
		{name: "audience string", token: jwt(`{"exp":1000060,"aud":"lameduck"}`), ok: true},
		{name: "wrong audience", token: jwt(`{"exp":1000060,"aud":["other"]}`)},
		{name: "expired", token: jwt(`{"exp":1000000,"aud":"lameduck"}`)},
		{name: "no expiry", token: jwt(`{"aud":"lameduck"}`)},
		{name: "not json", token: jwt(`garbage`)},
		{name: "two parts", token: "abc.def"},
		{name: "bad encoding", token: "a.!!!.c"},
		{name: "oversized", token: jwt(fmt.Sprintf(`{"exp":1000060,"aud":"lameduck","x":%q}`, strings.Repeat("x", maxTokenLength)))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expiry, err := unverifiedExpiry(test.token, "lameduck", now)
			if test.ok != (err == nil) {
				t.Fatalf("err = %v; want ok = %v", err, test.ok)
			}
			if test.ok && !expiry.Equal(time.Unix(1_000_060, 0)) {
				t.Fatalf("expiry = %v; want %v", expiry, time.Unix(1_000_060, 0))
			}
		})
	}
}
