<?php

namespace NightOwl\Support;

/**
 * Hardcoded cache-key templating: collapse machine-generated key segments
 * (uuid, ulid, hex digests, integers, emails, datetimes, session ids) into
 * placeholders so
 * the cache rollup groups by key SHAPE instead of key instance. `user:8213:
 * profile` and `user:44:profile` become one `user:{int}:profile` group; keys
 * with no variable segment pass through byte-identical.
 *
 * Why: the cache rollup is the only one keyed on a raw value, and on tenants
 * with unbounded keys it approaches the raw row count — measured 14.7M rollup
 * rows at 1.07 rows per key on the tenant whose 24h cache list tripped the 20s
 * statement_timeout. Grouping by shape is the house pattern everywhere else
 * (routes → /api/users/{id}, queries → fingerprints, outgoing → domains);
 * cache was the documented exception (migration 000037's own warning).
 *
 * Contract — the drain must NEVER wedge, so template() is TOTAL:
 *  - never throws (whole body catch → '{unparsed}'), never returns null;
 *  - always valid UTF-8 within 255 bytes (char-boundary backoff — input may
 *    already be invalid UTF-8: Laravel's Str::restrict truncates at 255 BYTES);
 *  - zero regex: every stage is a single left-to-right byte scanner, so there
 *    is no backtracking class at all, and no preg_* with /u (which returns
 *    null on malformed UTF-8 and would break totality);
 *  - linear in input length (validated to 100KB inputs pre-rewrite).
 *
 * Stage ORDER is load-bearing: datetimes/emails/uuids contain ':' '.' '-' '@'
 * and must be masked BEFORE stage 4 splits on those delimiters, or an ISO-8601
 * timestamp shatters into `{int}-{int}-{int}` soup.
 *
 * Conservative by design — only classes recognisable with high confidence
 * collapse; anything else passes through literally, including bare high-entropy
 * tokens (nanoid, and base62 ids of any length but one). There is still no
 * general entropy backstop: with no per-app config a false collapse would be
 * imposed on every customer, and the measured backstop designs all destroyed
 * real vocabulary.
 *
 * The one exception is the Laravel session id, which is not an entropy guess
 * but a shape the framework defines exactly — see {session} in classify(). It
 * earned the exception in the field, at 96.8% of one customer's cache rollup.
 *
 * The PHP form is the ONLY implementation: the rollup's SQL group form reads
 * the key_pattern column this code populated at ingest (COALESCE(key_pattern,
 * key, '')), so PHP/SQL equivalence holds by construction and no regex ever
 * crosses the PCRE/POSIX boundary.
 */
final class CacheKeyTemplate
{
    private const MAX_BYTES = 255;

    /** Crockford base32 (no I, L, O, U) — ULIDs are 26 of these, uppercase. */
    private const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

    public static function template(string $raw): string
    {
        try {
            if ($raw === '') {
                return '';
            }

            $s = self::maskDateTimes($raw);
            $s = self::maskEmails($s);
            $s = self::maskUuids($s);
            $s = self::classifyTokens($s);

            return self::bound($s);
        } catch (\Throwable) {
            return '{unparsed}';
        }
    }

    /** A "word byte" glues tokens together: ASCII alnum, or any byte >= 0x80. */
    private static function isWordByte(int $b): bool
    {
        return ($b >= 0x30 && $b <= 0x39)  // 0-9
            || ($b >= 0x41 && $b <= 0x5A)  // A-Z
            || ($b >= 0x61 && $b <= 0x7A)  // a-z
            || $b >= 0x80;
    }

    private static function isDigit(int $b): bool
    {
        return $b >= 0x30 && $b <= 0x39;
    }

    private static function isHexByte(int $b): bool
    {
        return self::isDigit($b)
            || ($b >= 0x41 && $b <= 0x46)   // A-F
            || ($b >= 0x61 && $b <= 0x66);  // a-f
    }

    /**
     * Stage 1 — mask ISO-8601-ish dates/datetimes BEFORE any delimiter split.
     * YYYY-MM-DD (month 1-12, day 1-31), optional time (T/t/space/_ HH:MM with
     * ranges, optional :SS, optional .fff, optional zone), with word-boundary
     * checks on both sides so half of a longer token is never swallowed.
     */
    private static function maskDateTimes(string $s): string
    {
        $n = strlen($s);
        $out = '';
        $i = 0;

        while ($i < $n) {
            $b = ord($s[$i]);
            $prevIsWord = $i > 0 && self::isWordByte(ord($s[$i - 1]));

            if (self::isDigit($b) && ! $prevIsWord) {
                [$len, $hasTime] = self::matchDate($s, $i);
                if ($len > 0) {
                    $after = $i + $len;
                    if ($after >= $n || ! self::isWordByte(ord($s[$after]))) {
                        $out .= $hasTime ? '{datetime}' : '{date}';
                        $i = $after;

                        continue;
                    }
                }
            }

            $out .= $s[$i];
            $i++;
        }

        return $out;
    }

    /** @return array{0: int, 1: bool} matched length (0 = no match) and whether a time part matched */
    private static function matchDate(string $s, int $i): array
    {
        $n = strlen($s);
        $digits = static function (int $at, int $count) use ($s, $n): ?int {
            if ($at + $count > $n) {
                return null;
            }
            $v = 0;
            for ($k = 0; $k < $count; $k++) {
                $b = ord($s[$at + $k]);
                if (! self::isDigit($b)) {
                    return null;
                }
                $v = $v * 10 + ($b - 0x30);
            }

            return $v;
        };

        // YYYY-MM-DD with range validation (rejects 2026-13-45).
        if ($digits($i, 4) === null || ($s[$i + 4] ?? '') !== '-') {
            return [0, false];
        }
        $month = $digits($i + 5, 2);
        if ($month === null || $month < 1 || $month > 12 || ($s[$i + 7] ?? '') !== '-') {
            return [0, false];
        }
        $day = $digits($i + 8, 2);
        if ($day === null || $day < 1 || $day > 31) {
            return [0, false];
        }

        $p = $i + 10;

        // Optional time part.
        $sep = $s[$p] ?? '';
        if ($sep !== 'T' && $sep !== 't' && $sep !== ' ' && $sep !== '_') {
            return [$p - $i, false];
        }
        $hour = $digits($p + 1, 2);
        $minute = $digits($p + 4, 2);
        if ($hour === null || $hour > 23 || ($s[$p + 3] ?? '') !== ':' || $minute === null || $minute > 59) {
            return [$p - $i, false]; // date matched; the "time" wasn't one
        }
        $p += 6;

        // Optional :SS (leap-second tolerant).
        if (($s[$p] ?? '') === ':') {
            $sec = $digits($p + 1, 2);
            if ($sec !== null && $sec <= 60) {
                $p += 3;
            }
        }

        // Optional fractional seconds: '.' + 1..9 digits (bounded count, no quantifier).
        if (($s[$p] ?? '') === '.') {
            $k = 0;
            while ($k < 9 && $p + 1 + $k < strlen($s) && self::isDigit(ord($s[$p + 1 + $k]))) {
                $k++;
            }
            if ($k > 0) {
                $p += 1 + $k;
            }
        }

        // Optional zone: Z/z, or +/- HH[:MM | MM].
        $z = $s[$p] ?? '';
        if ($z === 'Z' || $z === 'z') {
            $p += 1;
        } elseif ($z === '+' || $z === '-') {
            $zh = $digits($p + 1, 2);
            if ($zh !== null) {
                if (($s[$p + 3] ?? '') === ':' && $digits($p + 4, 2) !== null) {
                    $p += 6;
                } elseif ($digits($p + 3, 2) !== null) {
                    $p += 5;
                } else {
                    $p += 3;
                }
            }
        }

        return [$p - $i, true];
    }

    /**
     * Stage 2 — mask emails before '.' and '@' splitting. Expand left from '@'
     * over local-part bytes (bounded by what this scan already emitted), right
     * over domain bytes; require a dotted, >=2-alpha TLD.
     */
    private static function maskEmails(string $s): string
    {
        $n = strlen($s);
        $out = '';
        $flushed = 0; // everything before this offset is already in $out

        $isLocal = static fn (int $b): bool => self::isWordByte($b) && $b < 0x80
            || in_array(chr($b), ['.', '_', '%', '+', '-'], true);
        $isDomain = static fn (int $b): bool => (self::isWordByte($b) && $b < 0x80) || $b === 0x2E || $b === 0x2D; // alnum . -
        $isAlnum = static fn (int $b): bool => self::isWordByte($b) && $b < 0x80;

        for ($i = 0; $i < $n; $i++) {
            if ($s[$i] !== '@') {
                continue;
            }

            // Left expansion, bounded by the last flush (linearity guarantee).
            $start = $i;
            while ($start > $flushed && $isLocal(ord($s[$start - 1]))) {
                $start--;
            }
            while ($start < $i && ! $isAlnum(ord($s[$start]))) {
                $start++;
            }
            if ($start === $i) {
                continue; // no local part
            }

            // Right expansion.
            $end = $i + 1;
            while ($end < $n && $isDomain(ord($s[$end]))) {
                $end++;
            }
            $domEnd = $end;
            while ($domEnd > $i + 1 && ! $isAlnum(ord($s[$domEnd - 1]))) {
                $domEnd--; // trim trailing dots/hyphens
            }
            $domain = substr($s, $i + 1, $domEnd - $i - 1);
            $lastDot = strrpos($domain, '.');
            if ($domain === '' || $lastDot === false || $lastDot === 0) {
                continue;
            }
            $tld = substr($domain, $lastDot + 1);
            if (strlen($tld) < 2 || ! ctype_alpha($tld)) {
                continue;
            }

            $out .= substr($s, $flushed, $start - $flushed).'{email}';
            $flushed = $domEnd;
            $i = $domEnd - 1;
        }

        return $out.substr($s, $flushed);
    }

    /** Stage 3 — mask 8-4-4-4-12 hex UUIDs (any case) before '-' splitting. */
    private static function maskUuids(string $s): string
    {
        $n = strlen($s);
        $out = '';
        $i = 0;

        while ($i < $n) {
            $b = ord($s[$i]);
            $prevIsWord = $i > 0 && self::isWordByte(ord($s[$i - 1]));

            if (self::isHexByte($b) && ! $prevIsWord && $i + 36 <= $n) {
                $ok = true;
                $groups = [8, 4, 4, 4, 12];
                $p = $i;
                foreach ($groups as $gi => $len) {
                    for ($k = 0; $k < $len; $k++) {
                        if (! self::isHexByte(ord($s[$p + $k]))) {
                            $ok = false;
                            break 2;
                        }
                    }
                    $p += $len;
                    if ($gi < 4) {
                        if (($s[$p] ?? '') !== '-') {
                            $ok = false;
                            break;
                        }
                        $p++;
                    }
                }
                if ($ok) {
                    $after = $i + 36;
                    $afterByte = $after < $n ? ord($s[$after]) : null;
                    if ($afterByte === null || (! self::isWordByte($afterByte) && $s[$after] !== '-')) {
                        $out .= '{uuid}';
                        $i = $after;

                        continue;
                    }
                }
            }

            $out .= $s[$i];
            $i++;
        }

        return $out;
    }

    /**
     * Stage 4 — split on every non-word byte (delimiters re-emitted verbatim,
     * so unclassified input round-trips byte-identical) and classify each
     * token: majority-invalid UTF-8 → {bin}; all-digit → {int} (before hex —
     * an all-digit run is an id far more often than a digest); 26-char
     * uppercase Crockford → {ulid}; >=8 all-hex not mixed-case → {hex}; else
     * the literal token (with invalid bytes dropped).
     */
    private static function classifyTokens(string $s): string
    {
        $n = strlen($s);
        $out = '';
        $i = 0;

        while ($i < $n) {
            $b = ord($s[$i]);
            if (! self::isWordByte($b)) {
                $out .= $s[$i];
                $i++;

                continue;
            }

            $start = $i;
            while ($i < $n && self::isWordByte(ord($s[$i]))) {
                $i++;
            }
            $out .= self::classify(substr($s, $start, $i - $start));
        }

        return $out;
    }

    private static function classify(string $t): string
    {
        $len = strlen($t);

        // High bytes: unicode word or binary junk?
        $hasHigh = false;
        for ($k = 0; $k < $len; $k++) {
            if (ord($t[$k]) >= 0x80) {
                $hasHigh = true;
                break;
            }
        }
        if ($hasHigh) {
            $bad = self::invalidUtf8ByteCount($t);
            if ($bad > 0 && $bad * 2 >= $len) {
                return '{bin}';
            }
            if ($bad > 0) {
                // Re-classify the repaired token — one stray byte glued to a
                // numeric/hex run must not defeat the collapse (and leaving
                // the repaired literal would break idempotence: a second pass
                // WOULD classify it). Depth-bounded: the repaired token is
                // valid UTF-8, so the recursive call takes the bad === 0 path.
                return self::classify(self::repairUtf8($t));
            }

            return $t;
        }

        $allDigit = true;
        for ($k = 0; $k < $len; $k++) {
            if (! self::isDigit(ord($t[$k]))) {
                $allDigit = false;
                break;
            }
        }
        if ($allDigit) {
            return '{int}';
        }

        if ($len === 26 && strspn($t, self::CROCKFORD) === 26) {
            return '{ulid}';
        }

        if ($len >= 8) {
            $allHex = true;
            $hasUpper = false;
            $hasLower = false;
            for ($k = 0; $k < $len; $k++) {
                $b = ord($t[$k]);
                if (! self::isHexByte($b)) {
                    $allHex = false;
                    break;
                }
                if ($b >= 0x41 && $b <= 0x46) {
                    $hasUpper = true;
                }
                if ($b >= 0x61 && $b <= 0x66) {
                    $hasLower = true;
                }
            }
            // Mixed case is nearly always an identifier, not a digest.
            if ($allHex && ! ($hasUpper && $hasLower)) {
                return '{hex}';
            }
        }

        // A 40-byte alphanumeric token carrying an upper, a lower AND a digit:
        // Laravel's own definition of a session id, minus the guard.
        // Illuminate\Session\Store::isValidId is `ctype_alnum($id) &&
        // strlen($id) === 40` (SESSION_ID_LENGTH), and a redis/memcached/apc/
        // array SESSION_DRIVER routes every session read and write through
        // CacheBasedSessionHandler into the cache repository, so each one emits
        // a cache event keyed by that id. Measured on a customer's live
        // database: 96.8% of the minute rollup's rows and 433k distinct keys,
        // adding ~400k hourly rollup rows a day, kept for hourly_days.
        //
        // The token is ASCII alnum by construction — high-byte tokens returned
        // above, and classifyTokens only ever hands over word-byte runs — so
        // ctype_alnum needs no test here, only the length.
        //
        // AFTER the hex rule on purpose: a 40-char sha1 is also 40 alnum bytes
        // and must stay {hex}. The three-class requirement is the
        // false-positive guard a general entropy backstop could not give —
        // vocabulary does not mix case and digits in a single 40-byte word — and
        // it costs only the ~0.09% of real session ids that draw no digit at
        // all. Those pass through literally, which is what every session id did
        // before this rule existed, so the cost is a smaller collapse and never
        // a wrong one.
        if ($len === 40) {
            $hasAlphaUpper = false;
            $hasAlphaLower = false;
            $hasDigit = false;
            for ($k = 0; $k < $len; $k++) {
                $b = ord($t[$k]);
                if ($b >= 0x41 && $b <= 0x5A) {
                    $hasAlphaUpper = true;
                } elseif ($b >= 0x61 && $b <= 0x7A) {
                    $hasAlphaLower = true;
                } elseif (self::isDigit($b)) {
                    $hasDigit = true;
                }
            }

            if ($hasAlphaUpper && $hasAlphaLower && $hasDigit) {
                return '{session}';
            }
        }

        return $t;
    }

    /** Bytes not part of a well-formed UTF-8 sequence. */
    private static function invalidUtf8ByteCount(string $t): int
    {
        $bad = 0;
        $n = strlen($t);
        $i = 0;
        while ($i < $n) {
            $len = self::utf8SequenceLength($t, $i);
            if ($len === 0) {
                $bad++;
                $i++;
            } else {
                $i += $len;
            }
        }

        return $bad;
    }

    /**
     * Length of the well-formed UTF-8 sequence at $i, or 0. Rejects
     * continuation-byte leads, overlongs, surrogates (ED A0..BF), out-of-range
     * (F0 < 90, F4 > 8F, F5+), and truncated tails.
     */
    private static function utf8SequenceLength(string $s, int $i): int
    {
        $n = strlen($s);
        $b0 = ord($s[$i]);

        if ($b0 < 0x80) {
            return 1;
        }
        if ($b0 < 0xC2) {
            return 0; // continuation byte or overlong lead (C0/C1)
        }

        $cont = static fn (int $at): bool => $at < $n && (ord($s[$at]) & 0xC0) === 0x80;

        if ($b0 <= 0xDF) {
            return $cont($i + 1) ? 2 : 0;
        }
        if ($b0 <= 0xEF) {
            if (! $cont($i + 1) || ! $cont($i + 2)) {
                return 0;
            }
            $b1 = ord($s[$i + 1]);
            if ($b0 === 0xE0 && $b1 < 0xA0) {
                return 0; // overlong
            }
            if ($b0 === 0xED && $b1 >= 0xA0) {
                return 0; // surrogate
            }

            return 3;
        }
        if ($b0 <= 0xF4) {
            if (! $cont($i + 1) || ! $cont($i + 2) || ! $cont($i + 3)) {
                return 0;
            }
            $b1 = ord($s[$i + 1]);
            if ($b0 === 0xF0 && $b1 < 0x90) {
                return 0; // overlong
            }
            if ($b0 === 0xF4 && $b1 > 0x8F) {
                return 0; // > U+10FFFF
            }

            return 4;
        }

        return 0;
    }

    /** Drop bytes that are not part of a well-formed sequence. */
    private static function repairUtf8(string $t): string
    {
        $out = '';
        $n = strlen($t);
        $i = 0;
        while ($i < $n) {
            $len = self::utf8SequenceLength($t, $i);
            if ($len === 0) {
                $i++;

                continue;
            }
            $out .= substr($t, $i, $len);
            $i += $len;
        }

        return $out;
    }

    /** Stage 5 — valid UTF-8, <= 255 bytes, cut on a character boundary. */
    private static function bound(string $s): string
    {
        $s = self::invalidUtf8ByteCount($s) > 0 ? self::repairUtf8($s) : $s;

        if (strlen($s) <= self::MAX_BYTES) {
            return $s;
        }

        $out = '';
        $i = 0;
        $n = strlen($s);
        while ($i < $n) {
            $len = self::utf8SequenceLength($s, $i);
            $len = $len > 0 ? $len : 1;
            if (strlen($out) + $len > self::MAX_BYTES) {
                break;
            }
            $out .= substr($s, $i, $len);
            $i += $len;
        }

        return $out;
    }
}
