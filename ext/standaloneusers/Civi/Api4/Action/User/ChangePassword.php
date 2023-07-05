<?php

namespace Civi\Api4\Action\User;

use Civi\Standalone\Security;
use Civi\Api4\User;

class ChangePassword extends \Civi\Api4\Generic\AbstractAction {

  /**
   * Existing password. Must be provided when changing own password.
   *
   * @var string|null
   */
  protected $existingPassword;

  /**
   * New password
   *
   * @var string
   */
  protected $newPassword;

  /**
   * UserID of user whose password is to be changed.
   *
   * @var int
   */
  protected $userID;

  public function _run(\Civi\Api4\Generic\Result $result) {
    global $loggedInUser;
    if (empty($loggedInUser)) {
      throw new \API_Exception("User not logged in");
    }

    $security = Security::singleton();

    if ($loggedInUser['id'] == $this->userID) {
      // Changing own password. Existing password must match.
      if (!$security->checkPassword($this->existingPassword ?? '', $loggedInUser['password'])) {
        throw new \API_Exception("Password mismatch");
      }
    }
    else {
      // Changing someone else's password? You'd better be special.
      if (!\CRM_Core_Permission::check(['cms:administer users', 'administer CiviCRM'])) {
        throw new \API_Exception("You don't have permission to do that.");
      }
    }

    // OK to change.
    User::update()
    ->addWhere('id', '=', $this->userID)
    ->addValue('password', $security->hashPassword($this->newPassword))
    ->execute();
  }

}
