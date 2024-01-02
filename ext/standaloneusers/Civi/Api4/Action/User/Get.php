<?php
namespace Civi\Api4\Action\User;

use Civi\Api4\Generic\Result;
use Civi\Api4\Generic\DAOGetAction;
use CRM_Core_Permission;
use CRM_Utils_System;

/**
 */
class Get extends DAOGetAction {

  /**
   * @throws \CRM_Core_Exception
   */
  public function _run(Result $result) {
    if ($this->getCheckPermissions()) {
      // We need to check permissions for this User.get request.
      // Fun fact: CRM_Core_Permission::check itself does an User.get API call, but without checkPermissions,
      // so don't call that without checkPermissions or you'll have an infinte loop.
      if (!CRM_Core_Permission::check('cms:administer users')) {
        // A user without administer users permission is potentially requesting record(s)
        // other than their own. Limit to their own.
        $this->where[] = ['id', '=', (int) CRM_Utils_System::getLoggedInUfID()];
      }
      // Never allow access to various fields.
      $this->expandSelectClauseWildcards();
      $this->select = array_values(array_diff($this->select, ['hashed_password', 'password_reset_token']));
    }
    return parent::_run($result);
  }

}
