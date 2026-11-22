# Security policy

hoardDB holds people's data, so a vulnerability report is the most useful thing a
stranger can send us. Thank you for sending one, and for sending it privately.

## Reporting a vulnerability

**Do not open a public issue, discussion or pull request for a security
problem.** A public report of an unfixed vulnerability is a roadmap for
attackers.

Send your report to:

> **admin@hoarddb.com**

Please include:

- what you found, and the version or commit it affects (`hoarddb-server -v`
  prints both);
- steps to reproduce it, as short as you can make them, or a proof of concept;
- what an attacker gains: reading data, writing data, taking a node down, or
  something else;
- whether you have told anyone else, and any date you need us to know about.

You will not be asked for anything else to get a report looked at: no account,
no form, no agreement.

## What happens next

1. We acknowledge the report within 3 working days.
2. We reproduce it, decide how serious it is, and tell you what we found, within 10
   working days.
3. We write a fix with a test that fails without it, and agree a disclosure date
   with you, no later than 90 days after the report and sooner for a problem that
   is being exploited.
4. We ship the fix, then describe the problem in public in the changelog and, if
   it warrants one, a write-up. Until a fix has shipped, nothing about the
   vulnerability is published in this repository: no bug file, no commit message
   that explains the mechanism.
5. We credit you in the changelog if you want that.

## Which versions are covered

hoardDB's first release is 1.1 and none has been tagged yet. Today, the latest commit on
the main branch is the only supported version. Once releases exist, this section
will name the release lines that receive security fixes.

## What is in scope

Anything that lets someone who should not, read or change data in a hoardDB node,
bypass its login or its roles, impersonate a node, take a node down with a small
request, or read key material. The server, the CLI, the container image and the
build and release scripts are all in scope.

Out of scope, because they are documented behaviour rather than defects:

- the CLI's `-insecure` flag skipping certificate verification;
- data being readable on a node whose encryption at rest is off (its default);
- a `dump` file being plaintext;
- the metrics and profiling endpoints being unauthenticated when you switch them
  on.

[docs/user/security.md](docs/security.md) lists these, and what is
protected, in plain language.

If you are unsure whether something is a vulnerability, report it. We would
rather read ten reports that turn out not to be than miss one that is.

## Testing in good faith

Please test against your own installation, not against someone else's. Do not
access, change or delete data that is not yours, and stop and tell us if you
reach data that is.
