<?php

namespace NightOwl\Support;

/**
 * The sslmode for the connections that leave TLS to libpq unless one was set:
 * Laravel's `nightowl` connection (nightowl:install, nightowl:migrate and the
 * other commands) and the health alerts.
 *
 * Those used to pass 'database.sslmode' always. That value falls back to
 * 'prefer' when NIGHTOWL_DB_SSLMODE is unset, so every install sent
 * sslmode=prefer, which overrode a PGSSLMODE environment variable (verify-full,
 * say) the commands had honoured before. With no sslmode in the DSN, libpq
 * applies PGSSLMODE, or its own default, which is also 'prefer'.
 *
 * The drain is not one of them. It has always connected with
 * 'database.sslmode' ('prefer' when nothing is set) and still does.
 */
final class DatabaseSslMode
{
    /**
     * The mode that was set, or null to leave it to libpq.
     *
     * NIGHTOWL_DB_SSLMODE first, read through the top-level 'explicit_sslmode'
     * key, which is null when the variable is unset. The 'database' array of a
     * published config replaces the package's wholesale (mergeConfigFrom is a
     * shallow merge), so the key has to live outside it to reach every install.
     *
     * Then 'database.sslmode', unless it holds 'prefer': that is the fallback
     * for an unset variable, and nothing tells it apart from a chosen 'prefer'.
     * Any other value was set on purpose, hard-coded in a published config or
     * at runtime. Leaving 'prefer' out costs nothing, since it is libpq's own
     * default when PGSSLMODE is unset.
     */
    public static function explicit(): ?string
    {
        $set = self::clean(config('nightowl.explicit_sslmode'));
        if ($set !== null) {
            return $set;
        }

        $configured = self::clean(config('nightowl.database.sslmode'));

        return $configured !== null && strtolower($configured) !== 'prefer' ? $configured : null;
    }

    private static function clean(mixed $value): ?string
    {
        if (! is_string($value) || trim($value) === '') {
            return null;
        }

        return trim($value);
    }
}
