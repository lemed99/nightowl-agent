<?php

namespace NightOwl\Agent;

use RuntimeException;

/**
 * Thrown when the agent cannot bind its TCP ingest port because the address is
 * already in use — most commonly because Nightwatch's own agent is running on
 * the shared default port (2407). Carries an operator-friendly message with the
 * fix; caught in AgentCommand so the CLI shows guidance instead of a raw stack
 * trace (and the failed bind isn't reported as telemetry on a restart loop).
 */
final class PortInUseException extends RuntimeException
{
}
