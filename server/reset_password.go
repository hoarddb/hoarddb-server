package server

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/config"
)

// argon2ParamsFromConfig is the configured work factor with the package
// defaults applied to any zero field.
func argon2ParamsFromConfig(cfg *config.Config) Argon2Params {
	return Argon2Params{
		Memory:      cfg.Auth.Argon2Memory,
		Iterations:  cfg.Auth.Argon2Iterations,
		Parallelism: cfg.Auth.Argon2Parallelism,
		SaltLen:     cfg.Auth.Argon2SaltLen,
		HashLen:     cfg.Auth.Argon2HashLen,
	}.withDefaults()
}

// PasswordCharacters returns the length of password in characters, the unit
// the password minimum is documented in. Bytes would count a two-byte
// character twice, so the CLI and the offline reset both call this and cannot
// disagree about a non-ASCII password (BUG-0505).
func PasswordCharacters(password string) int {
	return utf8.RuneCountInString(password)
}

// usersSealingMode is the mode the offline reset saves users.json under. The
// configured mode, as a running node uses it, except that a node master that
// guards sealed material is never written back in the clear because the
// command was given the keystore and not the mode (as credentialKeysFor does
// for the credential files).
func usersSealingMode(cfg *config.Config, credentials CredentialKeys) string {
	if !modeSealsStore(cfg.Encryption.Mode) && credentials.Sealed() {
		return "on"
	}
	return cfg.Encryption.Mode
}

// RefuseResetPasswordInCluster refuses the offline reset when cfg, the configuration the server
// loads, makes this node a cluster member. It decides by cluster.enabled only: every ring node, a
// node without a cluster included, has a ring/raft directory (spec 6.7, F8).
func RefuseResetPasswordInCluster(cfg *config.Config) error {
	if cfg.Cluster.Enabled {
		return ErrResetPasswordInCluster
	}
	return nil
}

// ResetPassword applies an offline password reset to {dataDir}/users.json and
// returns the term and version it wrote. It exists because a lost
// administrator password must have a documented recovery path (spec 7.1 rule
// 3).
//
// It bumps the term, not only the version: a stopped node cannot compute
// mastership, so a version-only reset inside its last-known term loses the
// fence comparison on rejoin and is discarded with no error — the symptom is
// "the reset didn't work" and nothing in the logs explains it. Bumping the
// term makes the reset win the fence instead.
//
// The caller is responsible for proving the server is stopped
// (CheckServerStopped) and for prompting; this function never sees the
// terminal.
func ResetPassword(cfg *config.Config, username, password string, force bool, logger *slog.Logger) (term, version uint64, err error) {
	if err := RefuseResetPasswordInCluster(cfg); err != nil {
		return 0, 0, err
	}
	if password == "" {
		return 0, 0, errors.New("reset password: the new password is empty")
	}
	if min := cfg.Auth.PasswordMinLength; min > 0 {
		if length := PasswordCharacters(password); length < min {
			return 0, 0, fmt.Errorf("reset password for %q: the password is %d characters, below the %d-character minimum; choose a longer one",
				username, length, min)
		}
	}

	usersPath := UsersFilePath(cfg.Storage.DataDir)

	// The keyring and the node master must exist before the file is read: once
	// users.json is encrypted, loading it needs the key material it was written
	// under, and reset-password runs with the server stopped (spec encryption
	// §7.2). Open them first and reuse them for the write, so the reset does not
	// rewrite the file under another key or as plaintext. The cluster token, the
	// signing key and users.json may be sealed under the node master; open them
	// with the keystore the configuration names (never creating a node master).
	credentials, err := OpenCredentialKeys(cfg)
	if err != nil {
		return 0, 0, fmt.Errorf("reset password: %w", err)
	}
	clusterToken, err := clusterAuthToken(cfg, credentials)
	if err != nil {
		return 0, 0, err
	}
	keyring, err := buildKeyring(keyringRequest{
		dataDir: cfg.Storage.DataDir, clusterToken: clusterToken, deploymentID: cfg.Auth.AuthDeploymentID,
		credentials: credentials, logger: logger,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("build auth signing keyring: %w", err)
	}
	key := keyring.Current()
	if len(key.Bytes) == 0 {
		return 0, 0, errors.New("reset password: no signing key available")
	}

	nodeMaster := credentials.nodeMaster
	loaded, err := LoadUsersConfig(usersPath, keyring, nodeMaster)
	if errors.Is(err, ErrNoUsersFile) {
		return 0, 0, fmt.Errorf("reset password for %q: %w (a reset needs an existing users.json; start the server once to create it)", username, err)
	}
	if err != nil {
		// The other way this load fails is key material, not a missing file: the
		// store is encrypted with the signing key, so a node that no longer
		// holds the key it was written under cannot read it. That error already
		// names the kid and the fix (encrypt-users-json spec §4) — appending the
		// "start the server once" hint would send the operator looking for a
		// file that is sitting right there.
		return 0, 0, fmt.Errorf("reset password for %q: %w", username, err)
	}
	if err := loaded.validateWith(argon2ParamsFromConfig(cfg)); err != nil {
		return 0, 0, fmt.Errorf("reset password: users file %s is invalid: %w", usersPath, err)
	}
	if !force && loaded.Master != "" {
		// The file names the master by its ring id, so compare with this
		// node's ring id: the configured id defaults to the host name, which
		// every node of a one-host cluster shares (BUG-0596).
		selfID, err := ringSelfID(cfg)
		if err != nil {
			return 0, 0, fmt.Errorf("reset password: cannot tell whether this node is the recorded master (%s): %w; pass --force if you have confirmed it is", loaded.Master, err)
		}
		if loaded.Master != selfID {
			return 0, 0, fmt.Errorf("reset password: this node (%s) is not the recorded master (%s); run the reset on the master, or pass --force if you have confirmed it is safe. "+
				"A users.json written before ring ids were recorded names the master by its host name; if this node is the master, pass --force once, and the next user change records %s",
				selfID, loaded.Master, selfID)
		}
	}

	pepper := PepperForKey(key, cfg.Auth.AuthDeploymentID)
	params := argon2ParamsFromConfig(cfg)

	salt := make([]byte, params.SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return 0, 0, fmt.Errorf("generate salt: %w", err)
	}
	clientHash := argon2.IDKey([]byte(password), salt,
		params.Iterations, params.Memory, params.Parallelism, uint32(params.HashLen))
	storedHash := PepperHash(pepper, clientHash)

	index := -1
	for i := range loaded.Users {
		if loaded.Users[i].Username == username {
			index = i
			break
		}
	}
	if index < 0 {
		return 0, 0, fmt.Errorf("reset password: user %q is not in %s; run 'user list' against the running server to see the users",
			username, usersPath)
	}

	loaded.Users[index].Credentials = CredentialMaterial{
		Algo:      "argon2id",
		Salt:      base64.StdEncoding.EncodeToString(salt),
		Hash:      base64.StdEncoding.EncodeToString(storedHash),
		Params:    params,
		PepperKID: key.ID,
	}
	loaded.Users[index].PasswordChangedAt = time.Now().UTC()
	loaded.Users[index].Epoch++
	// A node applying this file ends the user's open logins only when the
	// login generation moves, as in an online password change.
	loaded.Users[index].LoginGeneration++
	loaded.Term++
	loaded.Version++
	if err := loaded.Save(usersPath, usersSealing{Keyring: keyring, NodeMaster: nodeMaster, Mode: usersSealingMode(cfg, credentials), Argon2: params}); err != nil {
		return 0, 0, fmt.Errorf("reset password: %w", err)
	}

	// root.password (left by a version before the hash-only change and not yet
	// converted by a start) only ever seeds the configured root user (cfg.Server.RootUser,
	// "admin" by default — cmd/server applies the same fallback at startup); a
	// reset of any other user must leave it alone. This reset already runs with
	// the server stopped (the caller proves that via CheckServerStopped before
	// calling ResetPassword), so there is no concurrent reader of the bootstrap
	// file to race against.
	//
	// A removal failure is returned, not merely logged: the new credential is
	// already written to users.json by this point, but a stale bootstrap
	// secret left behind is exactly the defect this removal exists to close, and
	// log-and-continue let that failure go unreported. The term/version
	// already landed are named in the error text since the function's error
	// return path always reports zero.
	rootUser := cfg.Server.RootUser
	if rootUser == "" {
		rootUser = "admin"
	}
	if username == rootUser {
		if err := RetireRootPasswordFile(cfg.Storage.DataDir); err != nil {
			return 0, 0, fmt.Errorf("reset password for %q: users.json now holds the new credential (term %d, version %d), but the superseded bootstrap file could not be removed: %w",
				username, loaded.Term, loaded.Version, err)
		}
	}

	return loaded.Term, loaded.Version, nil
}
