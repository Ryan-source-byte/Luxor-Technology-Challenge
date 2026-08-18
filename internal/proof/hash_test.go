package proof

import "testing"

func TestResultMatchesChallengeExample(t *testing.T) {
	got := Result("123", "456")
	want := "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
	if got != want {
		t.Fatalf("Result() = %q, want %q", got, want)
	}
}
