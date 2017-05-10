<?php

namespace CompanyTech\DataWarehouse\Representations\Rule;

use CompanyTech\DataWarehouse\Exceptions\Definition\InvalidRuleDefinitionException;
use CompanyTech\DataWarehouse\Fabrics\TypeFabric;
use CompanyTech\DataWarehouse\Interfaces\Compare\Comparable;
use CompanyTech\DataWarehouse\Interfaces\Definition\DefinitionRule;
use CompanyTech\DataWarehouse\Interfaces\Definition\SoftValidationTags;
use CompanyTech\DataWarehouse\Interfaces\Type\Typeable;
#use CompanyTech\DataWarehouse\Exceptions\Validation\LineValidationException;



/**
 * Class SwitchRuleRepresentation
 * @package CompanyTech\DataWarehouse\Representations\Rule
 */
class SwitchRuleRepresentation extends AbstractRuleRepresentation implements DefinitionRule
{

    /**
     * Parses rule
     *
     * @param array $definition
     *
     * @return array|null
     *
     * @throws InvalidRuleDefinitionException
     */
    public function parse($definition)
    {

        if (!array_key_exists(SoftValidationTags::VALUE, $definition)) {
            throw new InvalidRuleDefinitionException(sprintf('Value for rule %s is not defined', self::getType()));
        }

        $switchType = $this->getSwitchType($definition[SoftValidationTags::VALUE]);

        if (!array_key_exists(SoftValidationTags::CASES, $definition)) {
            throw new InvalidRuleDefinitionException(sprintf('Cases for rule %s are not defined', self::getType()));
        }

        $cases = $definition[SoftValidationTags::CASES];

        foreach ($cases as $case) {
            if (!array_key_exists(SoftValidationTags::VALUE, $case)) {
                throw new InvalidRuleDefinitionException(sprintf(
                    'Case value for rule %s is not defined',
                    self::getType()
                ));
            }

            $caseType = $this->getCaseType($case[SoftValidationTags::VALUE]);

            $compare = $this->getCompareMethod($caseType, $case);

            if ($caseType->$compare($switchType)) {
                return $case;
            }
            else {
                self::$caseErrorStack[] = $case;
            }
        }

        return null;
    }


    /**
     * @param $valueDefinition
     *
     * @return Typeable|Comparable
     *
     * @throws InvalidRuleDefinitionException
     */
    private function getSwitchType($valueDefinition)
    {
        $type = $this->getVariableType($valueDefinition);
        if (!$this->isSupportsSwitchVariableType($type->getType())) {
            throw new InvalidRuleDefinitionException(sprintf('Value type %s is not supported', $type->getType()));
        }

        return $type;
    }

    /**
     * @param $valueDefinition
     *
     * @return Typeable|Comparable
     *
     * @throws InvalidRuleDefinitionException
     */
    private function getCaseType($valueDefinition)
    {
        $type = $this->getVariableType($valueDefinition);
        if (!$this->isSupportsCaseVariableType($type->getType())) {
            throw new InvalidRuleDefinitionException(sprintf(
                'Value type %s is not supported in case statement',
                $type->getType()
            ));
        }

        return $type;
    }

    /**
     * @param string $type
     *
     * @return bool
     */
    private function isSupportsCaseVariableType($type)
    {
        return in_array(
            $type,
            TypeFabric::getAvailableTypes()
        );
    }

    /**
     * @param string $type
     *
     * @return bool
     */
    private function isSupportsSwitchVariableType($type)
    {
        return in_array(
            $type,
            TypeFabric::getAvailableScalarTypes()
        );
    }

    /**
     * Returns type of rule
     *
     * @return string
     */
    public static function getType()
    {
        return SoftValidationTags::SWITCH_CASE;
    }
}
