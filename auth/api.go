/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import "net/http"

// User represents a user for authorization purposes
type User struct {
	Name          string
	IsMemberOfOrg bool
}

// Auth is the interface managing user auth flow
type Auth interface {
	AuthorizeOrRedirect(h http.Handler) http.Handler
	AuthorizeOrForbid(h http.Handler) http.Handler
	LoginHandler(w http.ResponseWriter, r *http.Request)
	LogoutHandler(w http.ResponseWriter, r *http.Request)
	AuthCallbackHandler(w http.ResponseWriter, r *http.Request)
	User(r *http.Request) *User
}
