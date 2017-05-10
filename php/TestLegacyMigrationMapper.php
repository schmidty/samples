<?php

namespace CompanyTech\DataWarehouse\Mappers;

use CompanyTech\DataWarehouse\Exceptions\InvalidTestLegacyMigrateException;

class TestLegacyMigrationMapper
{
    /**
     * @param $testName
     * @return string
     * @throws InvalidTestLegacyMigrateException
     */
    public function map( $sqlPath )
    {
        $queryString = '';

        try {
            $filepath = $sqlPath .'.sql';

            if (!$queryString = @file_get_contents($filepath)) {
                throw new InvalidTestLegacyMigrateException("No SQL string response");
            }
        } catch(\Exception $e) {
            error_log(sprintf("No SQL script for test found at '%s'",
                $filepath
            ));
        }

        return $queryString;
    }

}
