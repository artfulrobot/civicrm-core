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

/**
 *
 */
class CRM_Core_Permission_Standalone extends CRM_Core_Permission_Base {

  /**
   * permission mapping to stub check() calls
   * @var array
   */
  public $permissions = NULL;

  /**
   * Given a permission string, check for access requirements
   *
   * @param string $str
   *   The permission to check.
   * @param int $userId
   *
   * @return bool
   *   true if yes, else false
   */
  public function check($str, $userId = NULL) {
    if ($str == CRM_Core_Permission::ALWAYS_DENY_PERMISSION) {
      return FALSE;
    }
    if ($str == CRM_Core_Permission::ALWAYS_ALLOW_PERMISSION) {
      return TRUE;
    }

    if (class_exists(\Civi\Standalone\Security::class)) {
      return \Civi\Standalone\Security::singleton()->checkPermission($this, $str, $userId);
    }

    // return the stubbed permission (defaulting to true if the array is missing)
    return isset($this->permissions) && is_array($this->permissions) ? in_array($str, $this->permissions) : TRUE;
  }

  /**
   * Determine whether the permission store allows us to store
   * a list of permissions generated dynamically (eg by
   * hook_civicrm_permissions.)
   *
   * @return bool
   */
  public function isModulePermissionSupported() {
    return TRUE;
  }

  /**
   * @inheritdoc
   */
  public function upgradePermissions($permissions) {

    if (class_exists(\Civi\Standalone\Security::class)) {
      // Standalone does not store a list of permissions, so we don't need to
      // do anything with new permissions. But if any of our users have a
      // permission that no longer exists, we need to remove that now.
      $roles = \Civi\Api4\Role::get(FALSE)->execute();
      $validPermissions = array_keys($permissions);
      $records = [];
      $rolesAffected = [];
      foreach ($roles as $role) {
        $newPermissions = array_intersect($role['permissions'], $validPermissions);
        if ($newPermissions !== $role['permissions']) {
          $records[] = ['id' => $role['id'], 'permissions' => $newPermissions];
          $rolesAffected[] = $role['name'];
        }
      }
      if ($records) {
        \Civi\Api4\Role::save(FALSE)->setRecords($records)->execute();
        \Civi::log()->info("Removed old permissions from roles: " . implode(', ', $rolesAffected));
      }
    }
  }

}
