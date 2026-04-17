package redis

import "testing"

func TestDialAddress(t *testing.T) {
	tests := []struct {
		name        string
		address     string
		wantNetwork string
		wantAddress string
	}{
		{
			name:        "tcp address",
			address:     "localhost:6379",
			wantNetwork: "tcp",
			wantAddress: "localhost:6379",
		},
		{
			name:        "absolute unix socket",
			address:     "/var/run/redis/redis.sock",
			wantNetwork: "unix",
			wantAddress: "/var/run/redis/redis.sock",
		},
		{
			name:        "unix prefix",
			address:     "unix:/var/run/redis/redis.sock",
			wantNetwork: "unix",
			wantAddress: "/var/run/redis/redis.sock",
		},
		{
			name:        "unix url prefix",
			address:     "unix:///var/run/redis/redis.sock",
			wantNetwork: "unix",
			wantAddress: "/var/run/redis/redis.sock",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Redis{redisAddress: tc.address}

			network, address := r.dialAddress()
			if network != tc.wantNetwork {
				t.Fatalf("network = %q, want %q", network, tc.wantNetwork)
			}
			if address != tc.wantAddress {
				t.Fatalf("address = %q, want %q", address, tc.wantAddress)
			}
		})
	}
}
