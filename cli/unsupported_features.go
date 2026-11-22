package cli

// errAlterBucket explains the supported way to change a bucket's indexes.
func errAlterBucket(name string) error {
	return usagef(`altering bucket %q is not supported yet
       declare indexes when you create the bucket:
         create bucket %s { type: hash, indices: ["Email"] }
       to change indexes on an existing bucket, drop and recreate it —
       note that dropping deletes its data: 'drop bucket %s'`, name, name, name)
}

// errStreamUnsupported points at the blob read path that does work.
func errStreamUnsupported(name string) error {
	return usagef(`streaming is not available yet
       the transport defines OpStreamOpen but the server does not implement
       chunked reads. Read the whole blob instead:
         db.%s.get({_id_: "key"})              print it
         db.%s.get({_id_: "key", out: "f.bin"}) write it to a file`, name, name)
}

// errUnknownDateMethod lists the supported date methods when an unknown one is called.
func errUnknownDateMethod(method string) error {
	return usagef(`method %q is not a supported date method
       supported methods:
         getTime()                     return ms since epoch (int64)
         toISOString()                 return RFC 3339 string
         add(<n>, "<unit>")            add duration (unit: ms|s|m|h|d|w)
         trunc("<unit>")               truncate to unit (unit: ms|s|m|h|d|w)
         format("<pattern>")           format with %%-specifiers`, method)
}

// errNamedTimeZone reports that a named time zone is not supported, and names the offset form as the fix (D-7).
func errNamedTimeZone(zoneName string) error {
	return usagef(`named time zone %q is not supported — use an offset instead, e.g. +02:00`, zoneName)
}

// errDateAddRejectsUnknownUnit reports that an unknown unit was passed to d.add().
func errDateAddRejectsUnknownUnit(unit string) error {
	return usagef(`add: unknown unit %q
       supported units: ms (millisecond), s (second), m (minute), h (hour), d (day), w (week)`, unit)
}
