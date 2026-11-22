package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/termio"
)

// ringPasswordSource is where a user statement takes a new password from when the statement
// stream does not carry it: -password-file, or -password-stdin (one script line, not two).
// warnings receives the note about a password file others can read.
type ringPasswordSource struct {
	file     string
	stdin    bool
	warnings io.Writer
}

// WithRingPasswordSource gives the session the -password-file and -password-stdin flags; the
// note about a password file readable beyond its owner goes to warnings (stderr in the binary).
func WithRingPasswordSource(file string, stdin bool, warnings io.Writer) RingOption {
	return func(s *RingSession) { s.newPassword = ringPasswordSource{file: file, stdin: stdin, warnings: warnings} }
}

// ringPromptPassword asks at the terminal, twice and with no echo.
func ringPromptPassword() (string, error) {
	return termio.ReadPasswordTwice("New password: ", "Repeat new password: ")
}

// ringPasswordUsage is a refusal of the password's source: nothing was sent (exit 1).
func ringPasswordUsage(format string, args ...any) *RingError {
	return &RingError{Message: fmt.Sprintf(format, args...), Exit: ExitUsage}
}

// newUserPassword returns the new password of user name and refuses one shorter than the
// server's minimum. A terminal is prompted twice with no echo; a piped run takes the lines that
// follow the statement (one with -password-stdin, two otherwise); -password-file works in both.
// The password is never put in a message.
func (s *RingSession) newUserPassword(users RingUsers, name string) (string, error) {
	var password string
	var err error
	s.tookPassword = true
	switch {
	case s.newPassword.stdin:
		password, err = s.passwordFromScript(1)
	case s.newPassword.file != "":
		password, err = s.newPassword.fromFile()
	case s.script != nil:
		password, err = s.passwordFromScript(2)
	default:
		password, err = s.promptPassword()
		if err != nil {
			// termio's texts ("the two passwords did not match; nothing was changed") name no password.
			return "", &RingError{Message: err.Error(), Exit: ExitRefused, Err: err}
		}
	}
	if err != nil {
		return "", err
	}
	return password, checkPasswordMinimum(users.PasswordMinLength(), name, password)
}

// checkPasswordMinimum applies the server's password_min_length, which it reports at login: the
// server cannot, because it receives only the derived credential (BUG-0493). Length counts
// characters, not bytes, as HOARDDB_PASSWORD_MIN_LENGTH does.
func checkPasswordMinimum(minimum int, name, password string) error {
	if minimum <= 0 {
		return ringRefusal("", "the server did not report its minimum password length, so the new password for user %q cannot be checked "+
			"and nothing was changed; use a hoarddb server of the same version as this CLI", name)
	}
	if length := utf8.RuneCountInString(password); length < minimum {
		return ringRefusal("", "the password for user %q is %d characters; this server requires at least %d. "+
			"Choose a longer password, or ask an administrator to change HOARDDB_PASSWORD_MIN_LENGTH; nothing was changed",
			name, length, minimum)
	}
	return nil
}

// passwordFromScript reads count password lines from the statement stream of a piped run and
// requires them to agree.
func (s *RingSession) passwordFromScript(count int) (string, error) {
	if s.script == nil {
		return "", ringPasswordUsage("--password-stdin reads the line after the statement in a piped script, and this session is a terminal; " +
			"leave the flag off to be prompted without echo, or use --password-file <0600 file>")
	}
	entries := make([]string, 0, count)
	for len(entries) < count {
		line, err := s.script()
		if err != nil {
			return "", ringPasswordUsage("the script ends where the new password should be (%d line(s) expected after the statement); "+
				"add the password line(s) after the statement, or use --password-file <0600 file>", count)
		}
		entries = append(entries, strings.TrimRight(line, "\r\n"))
	}
	for _, entry := range entries[1:] {
		if entry != entries[0] {
			return "", ringPasswordUsage("the next %d line(s) of the script were read as the password and do not agree, so nothing was changed; "+
				"if the script has no password lines, those lines were statements: put the password line(s) after the statement, "+
				"or use --password-file <0600 file>", count)
		}
	}
	if entries[0] == "" {
		return "", ringPasswordUsage("the next line of the script was read as the password and is empty, so nothing was changed; " +
			"if the script has no password lines, add them after the statement, or use --password-file <0600 file>")
	}
	return entries[0], nil
}

// fromFile reads the new password from -password-file, noting a mode that lets others read it.
func (p ringPasswordSource) fromFile() (string, error) {
	info, err := os.Stat(p.file)
	if err != nil {
		return "", ringPasswordUsage("read password file: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		_, _ = fmt.Fprintf(p.warnings, "warning: password file %s is mode %04o, readable beyond its owner\n", p.file, perm)
	}
	content, err := os.ReadFile(p.file)
	if err != nil {
		return "", ringPasswordUsage("read password file: %v", err)
	}
	password := strings.TrimSpace(string(content))
	if password == "" {
		return "", ringPasswordUsage("the password file %s is empty", p.file)
	}
	return password, nil
}

// strayPasswordLine refuses the line after a statement that took a new password when that line is
// not a statement. It is most likely a password line the source did not take (the flags change how
// many lines a script gives), so it is never quoted: the refusal would put it in a log.
func (s *RingSession) strayPasswordLine() *RingError {
	why := "a piped script gives the new password as the two lines after the statement, so a third password line runs as a statement. Remove the extra line"
	switch {
	case s.newPassword.stdin:
		why = "with --password-stdin the new password is the one line after the statement, so a second password line runs as a statement. Remove the extra line"
	case s.newPassword.file != "":
		why = "with --password-file the new password comes from the file, so a password line in the script runs as a statement. Remove the password lines from the script"
	}
	return &RingError{Code: "PARSE_ERROR", Exit: ExitUsage, Message: "the line after the last user statement is not a statement, and it is not shown because it may be a password: " +
		why + ". Nothing after it was run"}
}
