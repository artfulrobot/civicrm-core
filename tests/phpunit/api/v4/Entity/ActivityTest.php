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
 */

namespace api\v4\Entity;

use Civi\Api4\Contact;
use api\v4\Api4TestBase;
use Civi\Api4\Activity;
use Civi\Api4\ActivityContact;
use Civi\Test\TransactionalInterface;

/**
 * Assert that updating an activity does not affect the targets.
 *
 * This test was written specifically to test
 * https://lab.civicrm.org/dev/core/-/issues/1428
 *
 * @group headless
 */
class ActivityTest extends Api4TestBase implements TransactionalInterface {

  public function testActivityUpdate() {

    $meetingActivityTypeID = \Civi\Api4\OptionValue::get()
      ->addSelect('value')
      ->addWhere('option_group_id:name', '=', 'activity_type')
      ->addWhere('name', '=', 'Meeting')
      ->execute()->first()['value'];

    $domainContactID = \CRM_Core_BAO_Domain::getDomain()->contact_id;
    $c1 = Contact::create(FALSE)->addValue('first_name', '1')->execute()->first()['id'];
    $c2 = Contact::create(FALSE)->addValue('first_name', '2')->execute()->first()['id'];

    $activityID = Activity::create(FALSE)
      ->setValues([
        'target_contact_id'   => [$c1],
        'assignee_contact_id' => [$c2],
        'activity_type_id:name' => 'Meeting',
        'status_id:name' => 'Completed',
        'source_contact_id'   => $domainContactID,
        'subject'             => 'test activity',
      ])->execute()->first()['id'];

    // Activity create does not return a full record, so get the ID then do another get call...
    $activity = Activity::get(FALSE)
      ->addSelect('id', 'subject', 'activity_type_id')
      ->addWhere('id', '=', $activityID)
      ->execute()->first();
    $this->assertEquals($meetingActivityTypeID, $activity['activity_type_id']);
    $this->assertEquals('test activity', $activity['subject']);

    // Now check we have the correct target and assignees.
    $activityContacts = ActivityContact::get(FALSE)
      ->addWhere('activity_id', '=', $activityID)
      ->execute()
      ->indexBy('contact_id')->column('record_type_id');

    // 1 is assignee
    // 2 is added
    // 3 is target/with
    $expectedActivityContacts = [$c1 => 3, $c2 => 1, $domainContactID => 2];
    ksort($expectedActivityContacts);
    ksort($activityContacts);
    $this->assertEquals($expectedActivityContacts, $activityContacts, "ActivityContacts not as expected.");

    // Test we can update the activity.
    Activity::update(FALSE)
      ->addWhere('id', '=', $activityID)
      ->addValue('subject', 'updated subject')
      ->execute();

    // Repeat the tests.
    $activity = Activity::get(FALSE)
      ->addSelect('id', 'subject', 'activity_type_id')
      ->addWhere('id', '=', $activityID)
      ->execute()->first();
    $this->assertEquals($meetingActivityTypeID, $activity['activity_type_id']);
    $this->assertEquals('updated subject', $activity['subject'], "Activity subject was not updated correctly by Activity::update.");

    // Now check we have the correct target and assignees.
    $activityContacts = ActivityContact::get(FALSE)
      ->addWhere('activity_id', '=', $activityID)
      ->execute()
      ->indexBy('contact_id')->column('record_type_id');
    ksort($activityContacts);
    $this->assertEquals($expectedActivityContacts, $activityContacts, "ActivityContacts not as expected after update.");
  }

  public function testActivityWithCustomData() {

    // Create custom group, and a field.
    $groupId = \Civi\Api4\CustomGroup::save(FALSE)
    ->setMatch(['name'])
    ->setRecords([
      [
        'name' => 'test_custom_group',
        'title' => 'test_custom_group',
        'extends' => 'Activity',
        'collapse_display' => TRUE,
        'is_multiple' => FALSE,
        'is_reserved' => FALSE,
        'is_public' => FALSE,
        'is_reserved' => FALSE,
      ]
    ])
    ->execute()->first()['id'];
    $fieldId = \Civi\Api4\CustomField::save(FALSE)
    ->setMatch(['name'])
    ->setRecords([
      [
        'custom_group_id.name' => 'test_custom_group',
        'label' => 'test_custom_field',
        'name' => 'test_custom_field',
        'html_type' => 'Text',
        'data_type' => 'String',
        'is_required' => FALSE,
        'is_searchable' => TRUE,
        'is_search_range' => FALSE,
        'is_active' => TRUE,
        'is_view' => TRUE,
        'serialize' => 0,
      ]
    ])
    ->execute()->first()['id'];

    $meetingActivityTypeID = \Civi\Api4\OptionValue::get()
      ->addSelect('value')
      ->addWhere('option_group_id:name', '=', 'activity_type')
      ->addWhere('name', '=', 'Meeting')
      ->execute()->first()['value'];

    $domainContactID = \CRM_Core_BAO_Domain::getDomain()->contact_id;
    $c1 = Contact::create(FALSE)->addValue('first_name', '1')->execute()->first()['id'];

    $activityID = Activity::create(FALSE)
      ->setValues([
        'activity_type_id:name' => 'Meeting',
        'status_id:name' => 'Completed',
        'target_contact_id'   => $c1,
        // 'activity_type_id'    => $meetingActivityTypeID,
        'source_contact_id'   => $domainContactID,
        'subject'             => 'test activity',
        'test_custom_group.test_custom_field' => 'fidget',
        'details' => '',
      ])->execute()->first()['id'];

    // Activity create does not return a full record, so get the ID then do another get call...
    $activity = Activity::get(FALSE)
      ->addSelect('id', 'subject', 'activity_type_id', 'test_custom_group.test_custom_field')
      ->addWhere('id', '=', $activityID)
      ->execute()->first();
    $this->assertEquals($meetingActivityTypeID, $activity['activity_type_id']);
    $this->assertEquals('test activity', $activity['subject']);


    // Check the custom data.
    $this->assertEquals('fidget', $activity['test_custom_group.test_custom_field']);

    // Now try the update action.
    $activityID = Activity::update(FALSE)
      ->addWhere('id', '=', $activityID)
      ->setValues([
        'test_custom_group.test_custom_field' => 'wonkler',
      ])->execute()->first()['id'];
    $activity = Activity::get(FALSE)
      ->addSelect('id', 'subject', 'activity_type_id', 'test_custom_group.test_custom_field')
      ->addWhere('id', '=', $activityID)
      ->execute()->first();

    // Check the custom data.
    $this->assertEquals('wonkler', $activity['test_custom_group.test_custom_field']);

    \Civi\Api4\CustomField::delete(FALSE)->addWhere('id', '=', $fieldId)->execute();
    \Civi\Api4\CustomGroup::delete(FALSE)->addWhere('id', '=', $groupId)->execute();
  }

}
