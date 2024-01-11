<?php
/**
 * @package CRM
 * @copyright CiviCRM LLC https://civicrm.org/licensing
 */

use Civi\Api4\Event\AuthorizeRecordEvent;
use Civi\Api4\UserRole;
use Civi\Core\HookInterface;

/**
 * Business access object for the User entity.
 */
class CRM_Standaloneusers_BAO_User extends CRM_Standaloneusers_DAO_User implements HookInterface {

  /**
   * Event fired before an action is taken on a User record.
   * @param \Civi\Core\Event\PreEvent $event
   */
  public static function self_hook_civicrm_pre(\Civi\Core\Event\PreEvent $event) {
    if (
      in_array($event->action, ['create', 'edit'], TRUE) &&
      empty($event->params['when_updated'])
    ) {
      // Track when_updated.
      $event->params['when_updated'] = date('YmdHis');
    }
  }

  /**
   * Event fired after an action is taken on a User record.
   * @param \Civi\Core\Event\PostEvent $event
   */
  public static function self_hook_civicrm_post(\Civi\Core\Event\PostEvent $event) {
    // Handle virtual "roles" field (defined in UserSpecProvider)
    // @see \Civi\Api4\Service\Spec\Provider\UserSpecProvider
    if (
      in_array($event->action, ['create', 'edit'], TRUE) &&
      isset($event->params['roles']) && $event->id
    ) {
      if ($event->params['roles']) {
        $newRoles = array_map(function($role_id) {
          return ['role_id' => $role_id];
        }, $event->params['roles']);
        UserRole::replace(FALSE)
          ->addWhere('user_id', '=', $event->id)
          ->setRecords($newRoles)
          ->execute();
      }
      else {
        UserRole::delete(FALSE)
          ->addWhere('user_id', '=', $event->id)
          ->execute();
      }
    }
  }

  public static function updateLastAccessed() {
    $sess = CRM_Core_Session::singleton();
    $ufID = (int) $sess->get('ufID');
    CRM_Core_DAO::executeQuery("UPDATE civicrm_uf_match SET when_last_accessed = NOW() WHERE id = $ufID");
    $sess->set('lastAccess', time());
  }

  public static function getPreferredLanguages(): array {
    return CRM_Core_I18n::uiLanguages(FALSE);
  }

  public static function getTimeZones(): array {
    $timeZones = [];
    foreach (\DateTimeZone::listIdentifiers() as $timezoneId) {
      $timeZones[$timezoneId] = $timezoneId;
    }
    return $timeZones;
  }

  /**
   * @see \Civi\Api4\Utils\CoreUtil::checkAccessRecord
   */
  public static function self_civi_api4_authorizeRecord(AuthorizeRecordEvent $e): void {
    $record = $e->getRecord();
    $action = $e->getActionName();
    $mayAdminUsers = \CRM_Core_Permission::check('cms:administer users');
    $isOwnUser = ((int) \CRM_Utils_System::getLoggedInUfID()) == $record['id'] ?? NULL;
    if ($action === 'delete') {
      if ($isOwnUser) {
        // Prevent users from deleting their own user account
        $e->setAuthorized(FALSE);
      }
      else {
        // Enforce administer user permission requirement
        $e->setAuthorized($mayAdminUsers);
      }
    }
    elseif ($action === 'update') {
      $e->setAuthorized($isOwnUser ?: $mayAdminUsers);
    }
    elseif ($action === 'create') {
      $e->setAuthorized($mayAdminUsers);
    }
    else {
      // Is there another write action we don't know about? If so, play it safe and say No.
      $e->setAuthorized(FALSE);
    }
  }

}
