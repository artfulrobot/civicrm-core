<?php
/*
 +--------------------------------------------------------------------+
 | Copyright CiviCRM LLC. All rights reserved.                        |
 |                                                                    |
 | This work is published under the GNU AGPLv3 license with some      |
 | permitted exceptions and without any warranty. For full license    |
 | and copyright information, see https://civicrm.org/licensing       |
 +--------------------------------------------------------------------+
 */

/**
 *
 * @package CRM
 * @copyright CiviCRM LLC https://civicrm.org/licensing
 * $Id$
 *
 */

/**
 * The CiviCRM duplicate discovery engine is based on an
 * algorithm designed by David Strauss <david@fourkitchens.com>.
 */
class CRM_Dedupe_BAO_Rule extends CRM_Dedupe_DAO_Rule {

  /**
   * Ids of the contacts to limit the SQL queries (whole-database queries otherwise)
   * @var array
   */
  public $contactIds = [];

  /**
   * Params to dedupe against (queries against the whole contact set otherwise)
   * @var array
   */
  public $params = [];

  /**
   * Return the SQL query for the given rule - either for finding matching
   * pairs of contacts, or for matching against the $params variable (if set).
   *
   * Example return value:
   *
   * SELECT t1.id id1, t2.id id2, 10 weight
   * FROM civicrm_contact t1
   * INNER JOIN civicrm_contact t2 ON (t1.household_name = t2.household_name AND t2.contact_type = 'Household')
   * WHERE t1.household_name IS NOT NULL
   *   AND t1.household_name <> ''
   *   AND t1.contact_type = 'Household'
   *   AND t1.id < t2.id
   *
   * Desired:
   *
   * 1. Create a query that quickly identifies contacts with duplicates for that field, and returns the first contact and its field value.
   * 2. Use (1) as a sub query and join on other contacts that match on that field.
   *
   * SELECT () id1, () id2, () weight
   * FROM (
   *   SELECT MIN(id) id, <value> value
   *   FROM <table>
   *   WHERE <value> is valid
   *     AND (contact type restriction)
   *  GROUP BY <value>
   *  HAVING COUNT(*) > 1
   * ) firstContactThatHasDupes
   * INNER JOIN <table> t2
   *   ON t2.<field> = firstContactThatHasDupes.value
   *      AND t2.contact_id > t1.contact_id AND (t2 contact type restriction)
   *
   *
   * @return string
   *   SQL query performing the search
   *
   * @throws \CRM_Core_Exception
   * @throws \CiviCRM_API3_Exception
   */
  public function sql() {
    if ($this->params &&
      (!array_key_exists($this->rule_table, $this->params) ||
        !array_key_exists($this->rule_field, $this->params[$this->rule_table])
      )
    ) {
      // if params is present and doesn't have an entry for a field, don't construct the clause.
      return NULL;
    }

    $ruleWeightSafe = (int) $this->rule_weight;
    if ($ruleWeightSafe === 0) {
      // No point doing this, it amounts to naught.
      return NULL;
    }

    // Create a template for the expression that extracts the value we want to compare
    // When we use this we swap out {alias} for the particular one.
    if (!$this->rule_length) {
      $valueExpression = "{alias}.{$this->rule_field}";
    }
    else {
      $valueExpression = "SUBSTR({alias}.$this->rule_field, 1, " . ((int) $this->rule_length) .")";
    }

    // Identify which contact type we're working with.
    $sql = "SELECT contact_type FROM civicrm_dedupe_rule_group WHERE id = {$this->dedupe_rule_group_id};";
    $contactType = CRM_Core_DAO::singleValueQuery($sql);
    // Check this is definitely SQL safe.
    if (!preg_match('/^(Individual|Organization|Household)$/', $contactType)) {
      throw new \Exception("Invalid contact type '$contactType'");
    }

    //
    // First generate a query that quickly identifies contacts that have duplicates.
    // Here we're trying to avoid doing a JOIN on big tables because those
    // quickly get out of hand - 10 records joined to themselves is 100
    // comparisons, but 10k records is 100M comparisons.
    //

    // This query calls the rule_table t1
    // Get an expression for the value we want to compare, for the base table.
    $val = str_replace('{alias}', 't1', $valueExpression);

    // Restrictions that apply to the rule table.
    // In the firstContactThatHasDupes query these are applied in the WHERE
    // but when joining the rule table for the 2nd time, these are applied
    // in the ON clause.
    $ruleTableRestrictions = [];

    // Depending on the rule, we discover:
    // - $contactIdField: an expression that identifeis the contact ID
    // - $ruleTableRestrictions: further restrictions.
    //
    switch ($this->rule_table) {
      case 'civicrm_contact':
        $contactIdField = 'id';
        $ruleTableRestrictions[] = "{alias}.contact_type = '$contactType'";
        break;

      case 'civicrm_address':
        $contactIdField = 'contact_id';
        break;

      case 'civicrm_email':
      case 'civicrm_im':
      case 'civicrm_openid':
      case 'civicrm_phone':
        $contactIdField = 'contact_id';
        break;

      case 'civicrm_note':
        $contactIdField = 'entity_id';
        break;

      default:
        // custom data tables
        if (preg_match('/^(civicrm|custom)_value_/', $this->rule_table)) {
          $contactIdField = 'entity_id';
        }
        else {
          throw new CRM_Core_Exception("Unsupported rule_table for civicrm_dedupe_rule.id of {$this->id}");
        }
        break;
    }


    // The primary query's WHERE uses the shared $ruleTableRestrictions
    // but adds some others too.
    $primaryWhere = $ruleTableRestrictions;

    // If it's a parametrised search and we have a value in the params, add that as
    // a restriction now.
    if (!empty($this->params[$this->rule_table][$this->rule_field])) {
      $match = $this->params[$this->rule_table][$this->rule_field];
      if ($this->rule_length) {
        $substr = function_exists('mb_substr') ? 'mb_substr' : 'substr';
        $match = $substr($match, 0, $this->rule_length);
      }
      $match = CRM_Core_DAO::escape($match, 'String');
      $primaryWhere[] = "{alias}.$valueExpression = '$match'";
    }

    // We need to figure out if the value we're matching on is valid,
    // typically that it is not null, empty, or zero.
    if ($this->getFieldType($this->rule_field) === CRM_Utils_Type::T_DATE) {
      // Avoid the 0000-00-00 date
      $primaryWhere[] = "{alias}.$this->rule_field > 1000101";
    }
    else {
      // This will rule out NULL and empty values.
      $primaryWhere[] = "{alias}.$this->rule_field <> ''";
    }

    // Glue together the primaryWhere values
    $primaryWhere = $primaryWhere
      ? 'WHERE ('
        . str_replace('{alias}', 't1', implode(') AND (', $primaryWhere))
        .  ") \n"
      : '';

    // Now compile the primary query.
    $firstContactWithDuplicates =
        "SELECT MIN($contactIdField) id, $val value \n"
      . "FROM {$this->rule_table} t1 \n"
      . $primaryWhere
      . "GROUP BY $val HAVING COUNT(*) > 1 ";

    // If we're only working on a subset of possible contacts, apply that
    // now to the inner query.
    if ($this->contactIds) {
      $cids = [];
      foreach ($this->contactIds as $cid) {
        $cids[] = CRM_Utils_Type::escape($cid, 'Integer');
      }
      $cids = implode(',', $cids);
      // Ensure that at least one of the contactIds is in the subset.
      $firstContactWithDuplicates .= " AND SUM($contactIdField IN ($cids)) > 0";
    }
    // $firstContactWithDuplicates is now complete.

    //
    // Next we need to join that table onto the 2nd copy of the rule table, t2.
    //


    // This query uses the shared restrictions in the ON clause.
    // Consolidate and replace {alias} with t2
    $onRestrictions = $ruleTableRestrictions;

    // We also want to restrict the duplicates such that their contact ID is
    // not the original one.
    // @todo this was avoided for parameterised calls - why? what are those?
    // Nb. If we're searching a subset of contacts, there's the chance that the
    // duplicate is found with a lower or higher contact ID, but if not, the duplicate
    // contact ID will be higher. This might offer a slight performance improvement.
    $op = empty($cids) ? '>' : '<>';
    $onRestrictions[] = " (t2.$contactIdField $op first_contact.id) \n";

    // We need a value expression to identify the data to match on, this time for t2.
    $val = str_replace('{alias}', 't2', $valueExpression);
    $onRestrictions[] = "first_contact.value = $val";

    // Special cases:
    if ($this->rule_table === 'civicrm_address') {
      // We need to further restrict the search to the same address location type.
      $onRestrictions[] = " (first_contact.location_type_id = t2.location_type_id) \n";

      if (!empty($this->params['civicrm_address']['location_type_id'])) {
        // The parameters specify a particular location type.
        $locTypeId = CRM_Utils_Type::escape(
          $this->params['civicrm_address']['location_type_id'], 'Integer', FALSE);
        if ($locTypeId) {
          $onRestrictions[] = "(t1.location_type_id = $locTypeId)";
        }
      }
    }

    // Combine to a string, and replace {alias} with 't2'
    $onRestrictions = '(' . implode(') AND (', $onRestrictions) . ')';
    $onRestrictions = str_replace('{alias}', 't2', $onRestrictions);

    // Finally compile the whole query:
    $sql = "SELECT first_contact.id id1, t2.$contactIdField id2, {$ruleWeightSafe} weight \n"
      . "FROM ($firstContactWithDuplicates) first_contact \n"
      . "INNER JOIN {$this->rule_table} t2 \n"
      . "ON $onRestrictions \n";

    return $sql;
  }

  /**
   * find fields related to a rule group.
   *
   * @param array $params contains the rule group property to identify rule group
   *
   * @return array
   *   rule fields array associated to rule group
   */
  public static function dedupeRuleFields($params) {
    $rgBao = new CRM_Dedupe_BAO_RuleGroup();
    $rgBao->used = $params['used'];
    $rgBao->contact_type = $params['contact_type'];
    $rgBao->find(TRUE);

    $ruleBao = new CRM_Dedupe_BAO_Rule();
    $ruleBao->dedupe_rule_group_id = $rgBao->id;
    $ruleBao->find();
    $ruleFields = [];
    while ($ruleBao->fetch()) {
      $field_name = $ruleBao->rule_field;
      if ($field_name == 'phone_numeric') {
        $field_name = 'phone';
      }
      $ruleFields[] = $field_name;
    }
    return $ruleFields;
  }

  /**
   * @param int $cid
   * @param int $oid
   *
   * @return bool
   */
  public static function validateContacts($cid, $oid) {
    if (!$cid || !$oid) {
      return NULL;
    }
    $exception = new CRM_Dedupe_DAO_Exception();
    $exception->contact_id1 = $cid;
    $exception->contact_id2 = $oid;
    //make sure contact2 > contact1.
    if ($cid > $oid) {
      $exception->contact_id1 = $oid;
      $exception->contact_id2 = $cid;
    }

    return !$exception->find(TRUE);
  }

  /**
   * Get the specification for the given field.
   *
   * @param string $fieldName
   *
   * @return array
   * @throws \CiviCRM_API3_Exception
   */
  public function getFieldType($fieldName) {
    $entity = CRM_Core_DAO_AllCoreTables::getBriefName(CRM_Core_DAO_AllCoreTables::getClassForTable($this->rule_table));
    if (!$entity) {
      // This means we have stored a custom field rather than an entity name in rule_table, figure out the entity.
      $entity = civicrm_api3('CustomGroup', 'getvalue', ['table_name' => $this->rule_table, 'return' => 'extends']);
      if (in_array($entity, ['Individual', 'Household', 'Organization'])) {
        $entity = 'Contact';
      }
      $fieldName = 'custom_' . civicrm_api3('CustomField', 'getvalue', ['column_name' => $fieldName, 'return' => 'id']);
    }
    $fields = civicrm_api3($entity, 'getfields', ['action' => 'create'])['values'];
    return $fields[$fieldName]['type'];
  }

}
