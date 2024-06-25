<?php

namespace Programster\Throttle\MySQL;

use Programster\Throttle\RateLimit;
use Programster\Throttle\RateLimitCollection;
use Programster\Throttle\RateLimitDriverInterface;

class RateLimitMysqlDriver implements RateLimitDriverInterface
{
    private readonly string $m_tableName;
    private \mysqli $m_db;


    public function __construct(\mysqli $db, string $tableName)
    {
        $this->m_db = $db;
        $this->m_tableName = $tableName;
    }


    /**
     * Processes the request, returning the rate limits that the request exceeds. If the array is empty
     * then the request did not exceed any rate limits.
     * @param string $requestIdentifier
     * @param RateLimitCollection $rateLimits
     * @return array
     * @throws \Exception
     */
    public function process(string $requestIdentifier, string $throttleId, RateLimitCollection $rateLimits) : RateLimitCollection
    {
        $exceededRateLimits = new RateLimitCollection();
        $escapedThrottleIdentifier = mysqli_escape_string($this->m_db, $throttleId);
        $escapedRequestIdentifier = mysqli_escape_string($this->m_db, $requestIdentifier);
        $this->pruneOldRequests($throttleId, $rateLimits);
        $this->storeRequest($throttleId, $requestIdentifier);

        foreach ($rateLimits as $rateLimit)
        {
            /* @var $rateLimit RateLimit */
            $minAllowedTime = time() - $rateLimit->timePeriodInSeconds;

            // now check if we have exceeded the limit.
            $query = "SELECT * FROM {$this->getEscapedTableName()} WHERE `requestor_id` = '{$escapedRequestIdentifier}' AND `throttle_id` = '{$escapedThrottleIdentifier}' AND `when` >= {$minAllowedTime}";
            $result = $this->m_db->query($query);

            if ($result->num_rows > $rateLimit->numAllowedRequests)
            {
                $exceededRateLimits->append($rateLimit);
            }
        }

        return $exceededRateLimits;
    }


    public function getCreateTableString() : string
    {
        return
            "CREATE TABLE {$this->getEscapedTableName()} (
                `id` int unsigned NOT NULL AUTO_INCREMENT,
                `requestor_id` varchar(255) NOT NULL,
                `throttle_id` varchar(255) NOT NULL,
                `when` INT UNSIGNED NOT NULL,
                PRIMARY KEY (`id`),
                INDEX idx_requestor_throttle (`requestor_id`,`throttle_id`),
                INDEX idx_when (`when`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    }


    private function pruneOldRequests(string $throttleId, RateLimitCollection $rateLimits) : void
    {
        $escapedThrottleIdentifier = mysqli_escape_string($this->m_db, $throttleId);
        $timeNow = time();
        $maxTimePeriod = 0;

        foreach ($rateLimits as $rateLimit)
        {
            $maxTimePeriod = max($maxTimePeriod, $rateLimit->timePeriodInSeconds);
        }

        $minAllowedTime = $timeNow - $maxTimePeriod;

        // purge old requests that don't matter any more.
        $query = "DELETE FROM {$this->getEscapedTableName()} WHERE `throttle_id` = '{$escapedThrottleIdentifier}' AND `when` < {$minAllowedTime}";
        $this->m_db->query($query);
    }


    private function storeRequest(string $throttleId, string $requestIdentifier) : void
    {
        $escapedThrottleIdentifier = mysqli_escape_string($this->m_db, $throttleId);
        $timeNow = time();
        $escapedRequestIdentifier = mysqli_escape_string($this->m_db, $requestIdentifier);
        $query = "INSERT INTO {$this->getEscapedTableName()} SET `requestor_id` = '{$escapedRequestIdentifier}', `throttle_id` = '{$escapedThrottleIdentifier}', `when`={$timeNow}";
        $this->m_db->query($query);
    }


    private function getEscapedTableName() : string { return mysqli_escape_string($this->m_db, $this->m_tableName); }
}