// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

/*
Package typeinfo contains code relating to Go types and their processing in
SQLair. As much as possible, reflection code is limited to this package. It
contains the logic for validating, extracting information from and scanning into
types passed by the user.
*/
package typeinfo
