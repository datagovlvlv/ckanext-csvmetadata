// Enable JavaScript's strict mode. Strict mode catches some common
// programming errors and throws exceptions, prevents some unsafe actions from
// being taken, and disables some confusing and bad JavaScript features.
"use strict";

jQuery(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip(); 
});

ckan.module('csvmetadata', function ($) {
  return {
    initialize: function () {
      this.el.change(function () {
            var check = $(this).prop("checked");
            if (check == false)
            {
              $.each( $('[data-is-name="true"]'), 
                  function (i, element) {$(element).val( $(element).attr("data-default-value") ) } )
            }
            else if (check == true)
            {
              $.each($('[data-is-name="true"]'), 
                  function(i, element){
                      $(element).attr("readonly", ""),
                      $(element).val( $(element).attr("data-header-value") ) } )
            }
        });
    }
  };
});
