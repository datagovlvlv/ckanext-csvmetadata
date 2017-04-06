"use strict";

ckan.module('csvmetadata-required', function ($) {
  return {
    initialize: function () {
      this.el.submit( function (e) {
              window.form_valid = true;
              $.each( $('[data-required="true"]'), 
                  function (i, element) { 
                        var value = $(element).val();
                        if (!value) {
                            $('html,body').animate({scrollTop: $(element).offset().top});
                            window.form_valid = false;
                            return false;
                        } 
                        else {return true;}
                  }
              )
              return window.form_valid;
      })
    }
  }
})
