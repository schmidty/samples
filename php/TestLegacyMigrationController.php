<?php

namespace App\Command\Controller;

use Cms\ExtensionManager\Extension\Xmanager;
use Zend\Mvc\Controller\AbstractActionController;
use CompanyTech\DataWarehouse\Migrations\TestLegacyMigration;

class TestLegacyMigrationController extends AbstractActionController
{

    protected $xmanager;

    public function __construct(Xmanager $xmanager) {
        $this->xmanager = $xmanager;
    }

    public function invokeAction()
    {
        $request = $this->getRequest();

        $testLegacyMigration = new TestLegacyMigration($request, $this->xmanager);
        $testLegacyMigration->execute();
    }
}
