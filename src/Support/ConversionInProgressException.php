<?php

namespace NightOwl\Support;

/**
 * Another process holds a table's partition-conversion advisory lock — a
 * deploy-pipeline `nightowl:partition` run still in flight while an operator
 * runs it by hand (the field incident: the loser's freshly-created {t}_pnew
 * was dropped out from under it and it died 42P01 mid index-replay). The
 * winner finishes the table; the refused caller retries once it has.
 */
final class ConversionInProgressException extends \RuntimeException {}
