// /ckanext/scheming/assets/js/scheming-ai-suggestions.js

// Global state for AI suggestions (similar to normal suggestions)
if (typeof window._schemingAiSuggestionsGlobalState === 'undefined') {
    window._schemingAiSuggestionsGlobalState = {
        datasetId: null,
        globalInitDone: false,
        pollAttempts: 0,
        isPolling: false,
        aiSuggestions: {}
    };
}

ckan.module('scheming-ai-suggestions', function($) {
  return {
    options: {
      pollingInterval: 2500,
      maxPollAttempts: 40,
      terminalStatuses: ['DONE', 'ERROR', 'FAILED']
    },
    
    initialize: function() {
      console.log("Initializing scheming-ai-suggestions module");
      
      var self = this;
      var el = this.el;
      var fieldName = $(el).data('field-name');
      var globalState = window._schemingAiSuggestionsGlobalState;
      
      // Hide button initially (will show when suggestions are ready)
      $(el).hide();
      
      // Get dataset ID if not already set
      if (!globalState.datasetId) {
        var $form = $(el).closest('form.dataset-form, form#dataset-edit');
        
        if ($form.length && $form.data('dataset-id')) {
          globalState.datasetId = $form.data('dataset-id');
        } else if ($form.length && $form.find('input[name="id"]').val()) {
          globalState.datasetId = $form.find('input[name="id"]').val();
        } else if ($('body').data('dataset-id')) {
          globalState.datasetId = $('body').data('dataset-id');
        } else {
          var pathArray = window.location.pathname.split('/');
          var datasetIndex = pathArray.indexOf('dataset');
          var editIndex = pathArray.indexOf('edit');
          if (datasetIndex !== -1 && editIndex !== -1 && editIndex === datasetIndex + 1 && pathArray.length > editIndex + 1) {
            var potentialId = pathArray[editIndex + 1];
            if (potentialId && potentialId.length > 5) {
              globalState.datasetId = potentialId;
            }
          }
        }
      }
      
      // Start polling if not already started
      if (!globalState.globalInitDone && globalState.datasetId) {
        globalState.globalInitDone = true;
        if (!globalState.isPolling) {
          this._pollForAiSuggestions();
        }
      }
      
      // Create custom popover when clicked
      $(el).on('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        console.log("AI suggestion button clicked for field:", fieldName);
        
        // Hide all other popovers first
        $('.ai-suggestion-popover').hide();
        
        // Get suggestion from global state (updated by polling)
        var suggestion = globalState.aiSuggestions[fieldName];
        var suggestionValue = suggestion ? suggestion.value : '';
        var suggestionSource = suggestion ? (suggestion.source + ' (Confidence: ' + (suggestion.confidence || 'N/A') + ')') : 'AI Generated';
        
        // Create popover if it doesn't exist yet
        var popoverId = 'ai-suggestion-popover-' + fieldName;
        if ($('#' + popoverId).length === 0) {
          createPopover(fieldName, suggestionValue, suggestionSource, $(el));
        } else {
          // Update and show existing popover
          updatePopover(popoverId, suggestionValue, suggestionSource);
          var $popover = $('#' + popoverId);
          positionPopover($popover, $(el));
          $popover.show();
        }
      });
    },
    
    _pollForAiSuggestions: function() {
      var self = this;
      var globalState = window._schemingAiSuggestionsGlobalState;
      
      if (!globalState.datasetId) {
        console.warn("AI Suggestions: No dataset ID found, cannot poll");
        return;
      }
      
      if (globalState.pollAttempts >= this.options.maxPollAttempts) {
        console.log("AI Suggestions: Max poll attempts reached");
        globalState.isPolling = false;
        return;
      }
      
      globalState.isPolling = true;
      globalState.pollAttempts++;
      
      console.log("AI Suggestions: Polling attempt " + globalState.pollAttempts);
      
      $.ajax({
        url: (ckan.SITE_ROOT || '') + '/api/3/action/package_show',
        data: { id: globalState.datasetId, include_tracking: false },
        dataType: 'json',
        cache: false,
        success: function(response) {
          if (response.success && response.result) {
            var datasetObject = response.result;
            var dppSuggestionsData = datasetObject.dpp_suggestions;
            
            if (dppSuggestionsData && dppSuggestionsData.ai_suggestions) {
              console.log("AI Suggestions: Found AI suggestions", dppSuggestionsData.ai_suggestions);
              
              // Store suggestions in global state
              globalState.aiSuggestions = dppSuggestionsData.ai_suggestions;
              
              // Show buttons for fields that have suggestions
              self._showAiSuggestionButtons(dppSuggestionsData.ai_suggestions);
              
              // Check if processing is complete
              var currentStatus = dppSuggestionsData.STATUS ? dppSuggestionsData.STATUS.toUpperCase() : null;
              
              if (currentStatus && self.options.terminalStatuses.includes(currentStatus)) {
                console.log("AI Suggestions: Processing complete with status " + currentStatus);
                globalState.isPolling = false;
                return;
              }
            }
            
            // Continue polling if not done
            if (globalState.pollAttempts < self.options.maxPollAttempts) {
              setTimeout(function() { self._pollForAiSuggestions(); }, self.options.pollingInterval);
            } else {
              globalState.isPolling = false;
            }
          }
        },
        error: function(jqXHR, textStatus, errorThrown) {
          console.error("AI Suggestions: Poll error", textStatus, errorThrown);
          
          if (globalState.pollAttempts < self.options.maxPollAttempts) {
            var nextPollDelay = self.options.pollingInterval * Math.pow(1.2, Math.min(globalState.pollAttempts, 7));
            setTimeout(function() { self._pollForAiSuggestions(); }, nextPollDelay);
          } else {
            globalState.isPolling = false;
          }
        }
      });
    },
    
    _showAiSuggestionButtons: function(aiSuggestions) {
      console.log("AI Suggestions: Showing buttons for available suggestions");
      
      // Show buttons for fields that have AI suggestions
      Object.keys(aiSuggestions).forEach(function(fieldName) {
        var $button = $('.ai-suggestion-btn[data-field-name="' + fieldName + '"]');
        if ($button.length > 0) {
          // Update button data attributes with new suggestion data
          var suggestion = aiSuggestions[fieldName];
          if (suggestion && suggestion.value) {
            $button.attr('data-suggestion-value', suggestion.value);
            $button.attr('data-suggestion-source', suggestion.source + ' (Confidence: ' + (suggestion.confidence || 'N/A') + ')');
            $button.show();
            console.log("AI Suggestions: Updated and showing button for field " + fieldName);
          }
        }
      });
    }
  };
});

// Add direct click handling outside of the module for buttons that might not have been initialized
$(document).ready(function() {
  console.log("Document ready - initializing AI suggestions click handlers");
  
  // Direct click handler for all AI suggestion buttons
  $(document).on('click', '.ai-suggestion-btn', function(e) {
    e.preventDefault();
    e.stopPropagation();
    
    var $button = $(this);
    var fieldName = $button.data('field-name');
    var globalState = window._schemingAiSuggestionsGlobalState;
    
    console.log("AI suggestion button clicked for field:", fieldName);
    
    // Hide all other popovers first
    $('.ai-suggestion-popover').hide();
    
    // Get suggestion from global state
    var suggestion = globalState.aiSuggestions[fieldName];
    var suggestionValue = suggestion ? suggestion.value : '';
    var suggestionSource = suggestion ? (suggestion.source + ' (Confidence: ' + (suggestion.confidence || 'N/A') + ')') : 'AI Generated';
    
    console.log("Suggestion value length:", suggestionValue.length);
    console.log("Suggestion source:", suggestionSource);
    
    // Create popover if it doesn't exist yet
    var popoverId = 'ai-suggestion-popover-' + fieldName;
    if ($('#' + popoverId).length === 0) {
      console.log("Creating new popover for field:", fieldName);
      createPopover(fieldName, suggestionValue, suggestionSource, $button);
    } else {
      // Update and show existing popover
      console.log("Showing existing popover for field:", fieldName);
      updatePopover(popoverId, suggestionValue, suggestionSource);
      positionPopover($('#' + popoverId), $button);
      $('#' + popoverId).show();
    }
  });
  
  // Close popover when clicking outside
  $(document).on('click', function(e) {
    if (!$(e.target).closest('.ai-suggestion-btn').length && 
        !$(e.target).closest('.ai-suggestion-popover').length) {
      $('.ai-suggestion-popover').hide();
    }
  });
});

// Function to create popover - moved outside the module
function createPopover(fieldName, suggestionValue, suggestionSource, $button) {
  var $field = $('#field-' + fieldName);
  var isSelect = $field.is('select');
  var isTextarea = $field.is('textarea');
  
  // Properly decode escaped newlines from JSON
  if (typeof suggestionValue === 'string') {
    // Replace escaped newlines with actual newlines
    suggestionValue = suggestionValue.replace(/\\n/g, '\n');
    // Also handle any double-escaped newlines
    suggestionValue = suggestionValue.replace(/\\\\n/g, '\n');
  }
  
  // Create popover HTML
  var $popover = $('<div id="ai-suggestion-popover-' + fieldName + '" class="ai-suggestion-popover custom-suggestion-popover"></div>');
  
  // Display value with proper formatting
  var displayValue = suggestionValue || 'No suggestion available';
  if (suggestionValue && suggestionValue.length > 300) {
    displayValue = suggestionValue.substring(0, 300) + '...';
  }
  
  // Escape HTML and convert newlines to <br> for display
  var escapedDisplayValue = displayValue
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
    .replace(/\n/g, '<br>');
  
  var popoverContent = 
    '<div class="suggestion-popover-content">' +
      '<div class="ai-suggestion-header">' +
        '<strong>AI Suggestion</strong>' +
        '<div class="ai-suggestion-source">' + suggestionSource + '</div>' +
      '</div>' +
      '<div class="suggestion-value">' + escapedDisplayValue + '</div>' +
      '<button class="suggestion-apply-btn ai-suggestion-apply-btn" ' +
        'data-target="field-' + fieldName + '" ' +
        'data-is-select="' + isSelect + '" ' +
        'data-is-textarea="' + isTextarea + '" ' +
        'data-is-valid="true">' +
        'Apply suggestion' +
      '</button>' +
    '</div>';
  
  $popover.html(popoverContent);
  $('body').append($popover);
  
  // Store the actual suggestion value (not escaped) in the button's data
  $popover.find('.suggestion-apply-btn').data('actualValue', suggestionValue);
  
  // Position the popover
  positionPopover($popover, $button);
  
  // Add event handler for apply button
  $popover.find('.suggestion-apply-btn').on('click', function() {
    console.log("Apply button clicked for field:", fieldName);
    var targetId = $(this).data('target');
    var value = $(this).data('actualValue'); // Use the stored actual value
    var isTextarea = $(this).data('is-textarea');
    var $target = $('#' + targetId);
    
    if ($target.length === 0) {
      console.error("Target field not found:", targetId);
      return;
    }
    
    console.log("Applying suggestion to field:", targetId);
    console.log("Value length:", value.length);
    
    // Apply the suggestion
    if (isTextarea || $target.is('textarea')) {
      $target.val(value);
      // Trigger change event
      $target.trigger('change');
      // Also trigger input event for any listeners
      $target.trigger('input');
    } else if ($target.is('select')) {
      // For select fields, try to match the value
      $target.val(value);
      $target.trigger('change');
    } else {
      $target.val(value);
      $target.trigger('change');
      $target.trigger('input');
    }
    
    // Add a success class for animation
    $target.addClass('suggestion-applied');
    setTimeout(function() {
      $target.removeClass('suggestion-applied');
    }, 1000);
    
    // Show success message
    showSuccessMessage($target);
    
    // Hide the popover
    $popover.hide();
  });
}

// Function to update existing popover content
function updatePopover(popoverId, suggestionValue, suggestionSource) {
  var $popover = $('#' + popoverId);
  if ($popover.length === 0) return;
  
  // Properly decode escaped newlines from JSON
  if (typeof suggestionValue === 'string') {
    suggestionValue = suggestionValue.replace(/\\n/g, '\n');
    suggestionValue = suggestionValue.replace(/\\\\n/g, '\n');
  }
  
  // Display value with proper formatting
  var displayValue = suggestionValue || 'No suggestion available';
  if (suggestionValue && suggestionValue.length > 300) {
    displayValue = suggestionValue.substring(0, 300) + '...';
  }
  
  // Escape HTML and convert newlines to <br> for display
  var escapedDisplayValue = displayValue
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
    .replace(/\n/g, '<br>');
  
  // Update popover content
  $popover.find('.ai-suggestion-source').text(suggestionSource);
  $popover.find('.suggestion-value').html(escapedDisplayValue);
  $popover.find('.suggestion-apply-btn').data('actualValue', suggestionValue);
}

// Function to position popover
function positionPopover($popover, $button) {
  var buttonPos = $button.offset();
  var parentWidth = $(window).width();
  var popoverWidth = Math.min(400, parentWidth - 40);
  
  var leftPos = buttonPos.left;
  if (leftPos + popoverWidth > parentWidth - 20) {
    leftPos = Math.max(20, parentWidth - popoverWidth - 20);
  }
  
  $popover.css({
    position: 'absolute',
    top: buttonPos.top + $button.outerHeight() + 10,
    left: leftPos,
    maxWidth: popoverWidth + 'px',
    zIndex: 1000,
    display: 'block'
  });
}

// Function to show success message
function showSuccessMessage($target) {
  var $successMsg = $('<div class="suggestion-success-message">✓ Suggestion applied!</div>');
  $successMsg.css({
    position: 'absolute',
    top: $target.offset().top - 30,
    left: $target.offset().left + $target.outerWidth() / 2,
    transform: 'translateX(-50%)',
    backgroundColor: 'rgba(42, 145, 52, 0.95)',
    color: 'white',
    padding: '6px 12px',
    borderRadius: '4px',
    fontSize: '13px',
    fontWeight: '600',
    zIndex: 1010,
    opacity: 0,
    transition: 'opacity 0.3s ease',
    boxShadow: '0 2px 8px rgba(0,0,0,0.2)'
  });
  $('body').append($successMsg);
  
  setTimeout(function() {
    $successMsg.css('opacity', '1');
  }, 10);
  
  setTimeout(function() {
    $successMsg.css('opacity', '0');
    setTimeout(function() {
      $successMsg.remove();
    }, 300);
  }, 1500);
}