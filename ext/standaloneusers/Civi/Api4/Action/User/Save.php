<?php
namespace Civi\Api4\Action\User;

use Civi\Api4\Generic\DAOSaveAction;
use Civi\Api4\Generic\Result;
use Civi\API\Exception\UnauthorizedException;
use CRM_Core_Permission;

class Save extends DAOSaveAction {
  use WriteTrait;

  /**
   * Run some permission checks before doing the save.
   */
  public function _run(Result $result) {
    if (empty($this->records)) {
      return;
    }
    if ($this->getCheckPermissions()) {
      $loggedInUfID = (int) \CRM_Utils_System::getLoggedInUfID();
      if (!$loggedInUfID) {
        // Never allow save if not logged in.
        throw new UnauthorizedException("User.save API call when not logged in.");
      }
      if (!CRM_Core_Permission::check('cms:administer users')) {
        // Non-admin users should only be allowed to affect their own record.
        foreach ($this->records as $row) {
          if (($row['id'] ?? 0) != $loggedInUfID) {
            throw new UnauthorizedException("User.save called for records other than logged-in user's");
          }
        }
      }
    }

    return parent::_run($result);
  }

}
