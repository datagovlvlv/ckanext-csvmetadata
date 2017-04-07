// Enable JavaScript's strict mode. Strict mode catches some common
// programming errors and throws exceptions, prevents some unsafe actions from
// being taken, and disables some confusing and bad JavaScript features.
"use strict";

function foreignkey_hide(el) {
  el = (typeof el !== 'undefined') ?  el : this;
  var check = $(this).prop("checked");
  var id = $(this).attr("data-foreignkey-id");
  if (check == false)
  {
    $.each( $('[data-foreignkey='+id+']'), 
        function (i, element) {
            $(element).closest("tr").attr("hidden", "");
            $(element).attr("hidden", "");
        }
    )
  }
  else if (check == true)
  {
    $.each( $('[data-foreignkey='+id+']'), 
      function (i, element) {
          $(element).removeAttr("hidden");
          $(element).closest("tr").removeAttr("hidden");
      }
    )
  }
}

$( document ).ready( $.each( $('[data-foreignkey-id]'), foreignkey_hide ) )

ckan.module('csvmetadata-foreignkey', function ($) {
  return {
    initialize: function () {
      this.el.change(foreignkey_hide);
    }
  }
})
