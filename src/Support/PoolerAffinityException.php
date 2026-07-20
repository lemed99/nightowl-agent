<?php

namespace NightOwl\Support;

/**
 * The conversion's connection moved between server backends mid-run — the
 * transaction-mode pooler case. The per-table SESSION advisory lock the
 * conversion believes it holds lives on a backend it is no longer talking to,
 * so nothing is serialising it against a peer, and its unlock will miss
 * (pg_advisory_unlock returns false with a WARNING), stranding the key until
 * that server backend is recycled.
 *
 * Typed separately from every other conversion error because it is the one
 * failure that is NOT per-table: the same pooler fronts every remaining raw
 * table, so nightowl:partition stops the whole run on it rather than marching
 * through the rest running destructive prep DDL unprotected and stranding a
 * lock each time. Deliberately NOT a subclass of ConversionInProgressException
 * — the command's catch order would then classify it as harmless contention,
 * which is precisely the mistake.
 */
final class PoolerAffinityException extends \RuntimeException {}
