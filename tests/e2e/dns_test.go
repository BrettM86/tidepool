//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestDNS_BridgedHandleAndTombstone(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "dns")
	username := h.uniqueName(t, "dnsuser")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	listener := h.newListener(t, cursor, colActorProfile)
	user.createPost(t, community.ID, "DNS actor "+h.suffix, "mint a bridged actor")
	profile := listener.await("DNS actor.profile for "+username, func(event *jsEvent) bool {
		name, _ := fieldOf(event.Commit.Record, "displayName")
		return event.Commit.Collection == colActorProfile && event.Commit.Operation == opCreate && name == username
	})
	handle := bridgedHandle(username)
	client := &dns.Client{Net: "udp", Timeout: 2 * time.Second}
	query := func(name string, questionType uint16) *dns.Msg {
		t.Helper()
		request := new(dns.Msg)
		request.SetQuestion(name, questionType)
		response, _, err := client.Exchange(request, dnsAddress())
		if err != nil {
			t.Fatalf("DNS query %s at %s: %v", name, dnsAddress(), err)
		}
		return response
	}

	txtName := "_atproto." + handle + "."
	txtResponse := query(txtName, dns.TypeTXT)
	if txtResponse.Rcode != dns.RcodeSuccess || len(txtResponse.Answer) != 1 {
		t.Fatalf("live TXT %s: rcode %d, answers %v; want NOERROR and one TXT", txtName, txtResponse.Rcode, txtResponse.Answer)
	}
	txt, ok := txtResponse.Answer[0].(*dns.TXT)
	if !ok || len(txt.Txt) != 1 || txt.Txt[0] != "did="+profile.Did {
		t.Fatalf("live TXT %s: answer %v, want did=%s", txtName, txtResponse.Answer[0], profile.Did)
	}

	aResponse := query(handle+".", dns.TypeA)
	if aResponse.Rcode != dns.RcodeSuccess || len(aResponse.Answer) != 1 {
		t.Fatalf("A %s: rcode %d, answers %v; want NOERROR and one A", handle, aResponse.Rcode, aResponse.Answer)
	}
	a, ok := aResponse.Answer[0].(*dns.A)
	if !ok || a.A.String() != dnsPublicIPv4() {
		t.Fatalf("A %s: answer %v, want %s", handle, aResponse.Answer[0], dnsPublicIPv4())
	}

	user.deleteAccount(t)
	deadline := time.NewTimer(eventTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		response := query(txtName, dns.TypeTXT)
		if response.Rcode != dns.RcodeSuccess {
			t.Fatalf("tombstoned TXT %s: rcode %d, want NOERROR", txtName, response.Rcode)
		}
		if len(response.Answer) == 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("tombstoned TXT %s still answers %v after %s", txtName, response.Answer, eventTimeout)
		}
	}
}
