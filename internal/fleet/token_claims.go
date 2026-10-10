package fleet

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

const maxTokenLength = 16 << 10

var errMalformedToken = errors.New("token is not a usable JWT")

type audiences []string

func (a *audiences) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*a = audiences{single}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return errMalformedToken
	}
	*a = many
	return nil
}

type tokenClaims struct {
	Expiry   int64     `json:"exp"`
	Audience audiences `json:"aud"`
}

func unverifiedExpiry(token, audience string, now time.Time) (time.Time, error) {
	if len(token) > maxTokenLength {
		return time.Time{}, errMalformedToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errMalformedToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errMalformedToken
	}
	var claims tokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, errMalformedToken
	}
	expiry := time.Unix(claims.Expiry, 0)
	if claims.Expiry == 0 || !now.Before(expiry) || !slices.Contains(claims.Audience, audience) {
		return time.Time{}, errMalformedToken
	}
	return expiry, nil
}
