package cli

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u52UserCall is one user change the fake was sent, with every argument the CLI chose.
type u52UserCall struct {
	method    string
	name      string
	password  *string
	roles     *[]ringclient.UserGrant
	enabled   *bool
	grant     ringclient.UserGrant
	dropFence int64
	expectSeq *int64
}

// u52Users is the fake's user store: what the zero-config binding keeps, enough for the
// statements' output, fences and refusals.
type u52Users struct {
	accounts map[string]ringclient.UserInfo
	revision int64
	dropSeq  int64
	// minLength is what PasswordMinLength reports, as the server's AUTH_OK does.
	minLength int
	changes   []u52UserCall
}

func (f *u52Fake) userStore() *u52Users {
	if f.users == nil {
		created := time.Date(2026, 9, 19, 6, 45, 1, 0, time.UTC)
		f.users = &u52Users{revision: 1, minLength: 12, accounts: map[string]ringclient.UserInfo{
			"root": {Name: "root", Roles: []ringclient.UserGrant{{Role: "root", DB: "*"}}, Enabled: true, Created: created, PasswordChanged: created, Revision: 1},
		}}
	}
	return f.users
}

func u52NoUser(name string) error {
	return &ringclient.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf("there is no user %q. Run 'show users' to list the users.", name)}
}

// change applies one recorded change: the fence and revision checks of the binding, then edit.
func (f *u52Fake) change(call u52UserCall, edit func(*ringclient.UserInfo) error) (ringclient.UserResult, error) {
	users := f.userStore()
	users.changes = append(users.changes, call)
	if err := f.fail(call.method); err != nil {
		return ringclient.UserResult{}, err
	}
	if call.expectSeq != nil && *call.expectSeq != users.revision {
		return ringclient.UserResult{}, &ringclient.Error{Code: cluster.CodeConflict, Message: fmt.Sprintf(
			"%s was not applied: the user store changed since this user was read (revision %d, now %d). Run 'user describe %s' and repeat the change against what it shows.",
			call.method, *call.expectSeq, users.revision, call.name)}
	}
	account, ok := users.accounts[call.name]
	if !ok {
		return ringclient.UserResult{}, u52NoUser(call.name)
	}
	if err := edit(&account); err != nil {
		return ringclient.UserResult{}, err
	}
	users.revision++
	account.Revision = users.revision
	users.accounts[call.name] = account
	return ringclient.UserResult{Revision: users.revision, Result: "done"}, nil
}

// CreateUser is the request create user makes.
func (f *u52Fake) CreateUser(_ context.Context, name, password string, roles []ringclient.UserGrant, expectDropSeq int64, _ ...ringclient.CallOption) (ringclient.UserResult, error) {
	users := f.userStore()
	users.changes = append(users.changes, u52UserCall{method: "CreateUser", name: name, password: &password, roles: &roles, dropFence: expectDropSeq})
	if err := f.fail("CreateUser"); err != nil {
		return ringclient.UserResult{}, err
	}
	if _, ok := users.accounts[name]; ok {
		return ringclient.UserResult{}, &ringclient.Error{Code: cluster.CodeUserExists, Message: fmt.Sprintf("user %q already exists; choose another name, or alter user %s ...", name, name)}
	}
	if expectDropSeq != users.dropSeq {
		return ringclient.UserResult{}, &ringclient.Error{Code: cluster.CodeConflict, Message: fmt.Sprintf(
			"create user %q was not applied: a user was dropped since the users were read (drop at %d, read at %d)", name, users.dropSeq, expectDropSeq)}
	}
	users.revision++
	users.accounts[name] = ringclient.UserInfo{Name: name, Roles: roles, Enabled: true, Revision: users.revision}
	return ringclient.UserResult{Revision: users.revision, Result: "done"}, nil
}

// AlterUser is the request alter user makes.
func (f *u52Fake) AlterUser(_ context.Context, name string, change ringclient.UserChange, _ ...ringclient.CallOption) (ringclient.UserResult, error) {
	return f.change(u52UserCall{method: "AlterUser", name: name, password: change.Password, roles: change.Roles, enabled: change.Enabled, expectSeq: change.ExpectSeq},
		func(account *ringclient.UserInfo) error {
			if change.Enabled != nil {
				account.Enabled = *change.Enabled
				account.Epoch++
			}
			if change.Roles != nil {
				account.Roles = *change.Roles
			}
			if change.Password != nil {
				account.Epoch++
			}
			return nil
		})
}

// DropUser is the request drop user makes.
func (f *u52Fake) DropUser(_ context.Context, name string, expectSeq *int64, _ ...ringclient.CallOption) (ringclient.UserResult, error) {
	result, err := f.change(u52UserCall{method: "DropUser", name: name, expectSeq: expectSeq}, func(*ringclient.UserInfo) error { return nil })
	if err == nil {
		delete(f.users.accounts, name)
		f.users.dropSeq++
	}
	return result, err
}

// GrantRole is the request grant role makes.
func (f *u52Fake) GrantRole(_ context.Context, name string, grant ringclient.UserGrant, expectSeq *int64, _ ...ringclient.CallOption) (ringclient.UserResult, error) {
	return f.change(u52UserCall{method: "GrantRole", name: name, grant: grant, expectSeq: expectSeq}, func(account *ringclient.UserInfo) error {
		for _, held := range account.Roles {
			if held == grant {
				return nil
			}
		}
		account.Roles = append(account.Roles, grant)
		return nil
	})
}

// RevokeRole is the request revoke role makes.
func (f *u52Fake) RevokeRole(_ context.Context, name string, grant ringclient.UserGrant, expectSeq *int64, _ ...ringclient.CallOption) (ringclient.UserResult, error) {
	return f.change(u52UserCall{method: "RevokeRole", name: name, grant: grant, expectSeq: expectSeq}, func(account *ringclient.UserInfo) error {
		kept := account.Roles[:0:0]
		for _, held := range account.Roles {
			if held != grant {
				kept = append(kept, held)
			}
		}
		account.Roles = kept
		return nil
	})
}

// ShowUsers lists the accounts in name order, as the server does.
func (f *u52Fake) ShowUsers(context.Context) (ringclient.UserList, error) {
	users := f.userStore()
	if err := f.fail("ShowUsers"); err != nil {
		return ringclient.UserList{}, err
	}
	list := ringclient.UserList{Revision: users.revision, DropSeq: users.dropSeq}
	for _, account := range users.accounts {
		account.Revision = users.revision
		list.Users = append(list.Users, account)
	}
	sort.Slice(list.Users, func(i, j int) bool { return list.Users[i].Name < list.Users[j].Name })
	return list, nil
}

// DescribeUser reads one account.
func (f *u52Fake) DescribeUser(_ context.Context, name string) (ringclient.UserDescription, error) {
	users := f.userStore()
	if err := f.fail("DescribeUser"); err != nil {
		return ringclient.UserDescription{}, err
	}
	account, ok := users.accounts[name]
	if !ok {
		return ringclient.UserDescription{}, u52NoUser(name)
	}
	account.Revision = users.revision
	return ringclient.UserDescription{User: account, DropSeq: users.dropSeq}, nil
}

// PasswordMinLength is the minimum the fake server reported at login.
func (f *u52Fake) PasswordMinLength() int { return f.userStore().minLength }
