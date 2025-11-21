if (typeof window._schemingSuggestionsGlobalState === 'undefined') {
    window._schemingSuggestionsGlobalState = {
        datasetId: null,
        globalInitDone: false,
        pollAttempts: 0,
        isPolling: false,
        isInitialLoadWithExistingSuggestions: false 
    };
}

ckan.module('scheming-suggestions', function($) {
    var esc = function(str) { return typeof str === 'string' ? $('<div>').text(str).html() : str; };
    var globalState = window._schemingSuggestionsGlobalState;

    return {
        options: {
            pollingInterval: 2500,
            maxPollAttempts: 40,
            initialButtonTitle: 'Suggestion available (loading...)',
            noSuggestionTitle: 'No suggestion currently available',
            suggestionReadyTitle: 'Suggestion Available',
            suggestionErrorTitle: 'Error in suggestion',
            processingMessage: '<span><i class="fa fa-spinner fa-spin"></i> Processing dataset, suggestions will appear shortly...</span>',
            statusProcessingTextPrefix: '<span><i class="fa fa-spinner fa-spin"></i> Status: ',
            statusDoneText: '<span><i class="fa fa-check-circle"></i> Suggestions processed. Fields updated.</span>',
            statusErrorText: '<span><i class="fa fa-exclamation-triangle"></i> Error processing suggestions. Status: ',
            timeoutMessage: '<span><i class="fa fa-exclamation-triangle"></i> Suggestions are taking longer than usual to process.</span>',
            errorMessage: '<span><i class="fa fa-times-circle"></i> Could not retrieve suggestions at this time.</span>',
            errorPrefix: '#ERROR!:', // User confirmed prefix
            terminalStatuses: ['DONE', 'ERROR', 'FAILED']
        },
        _popoverDivs: {},
        _originalButtonTitles: {},

        initialize: function() {
            var self = this;
            var el = this.el;
            var $form = $(el).closest('form');
            var foundDatasetId = null;

            // Dataset ID acquisition (run only if global ID not set)
            if (!globalState.datasetId) {
                if ($form.length && $form.data('dataset-id')) foundDatasetId = $form.data('dataset-id');
                else if ($form.length && $form.find('input[name="id"]').val()) foundDatasetId = $form.find('input[name="id"]').val();
                else if ($form.length && $form.find('input[name="pkg_name"]').val()) foundDatasetId = $form.find('input[name="pkg_name"]').val();
                else if ($('body').data('dataset-id')) foundDatasetId = $('body').data('dataset-id');
                else {
                    var pathArray = window.location.pathname.split('/');
                    var datasetIndex = pathArray.indexOf('dataset');
                    var editIndex = pathArray.indexOf('edit');
                    if (datasetIndex !== -1 && editIndex !== -1 && editIndex === datasetIndex + 1 && pathArray.length > editIndex + 1) {
                        var potentialId = pathArray[editIndex + 1];
                        if (potentialId && potentialId.length > 5) foundDatasetId = potentialId;
                    }
                }
                if (foundDatasetId) {
                    globalState.datasetId = foundDatasetId;
                }
            }

            var fieldName = $(el).data('field-name');
            if (!fieldName) return;

            this._originalButtonTitles[fieldName] = $(el).attr('title');
            $(el).attr('title', this.options.initialButtonTitle);
            var popoverId = 'custom-suggestion-popover-' + fieldName + '-' + Date.now();
            this._popoverDivs[fieldName] = $('<div class="custom-suggestion-popover" id="' + popoverId + '" style="display: none;"></div>').appendTo('body');
            $(el).hide();
            this._showFieldLoadingIndicator(el);

            if (!globalState.globalInitDone) {
                globalState.globalInitDone = true;
                if (!globalState.isPolling) this._pollForSuggestions(); // Start polling if not already
            }
            this._attachBaseEventHandlers(el, this._popoverDivs[fieldName], fieldName);
        },

        _showAllButtonsAsNoSuggestion: function() {
            var self = this;
             $('button[data-module="scheming-suggestions"]').each(function() {
                var currentButton = $(this);
                currentButton.attr('title', self.options.noSuggestionTitle).show();
                self._hideFieldLoadingIndicator(currentButton);
            });
        },
        _showFieldLoadingIndicator: function(buttonEl) {
            var $controlGroup = $(buttonEl).closest('.control-group.has-suggestion');
            if ($controlGroup.length === 0) $controlGroup = $(buttonEl).closest('.form-group.has-suggestion');
            var $label = $controlGroup.find('.control-label, .form-label').first();
            if ($label.length && $label.find('.suggestion-field-loader').length === 0) {
                $label.append('<span class="suggestion-field-loader">&nbsp;<i class="fa fa-circle-o-notch fa-spin fa-fw"></i></span>');
            }
        },
        _hideFieldLoadingIndicator: function(buttonEl) {
            var $controlGroup = $(buttonEl).closest('.control-group.has-suggestion');
             if ($controlGroup.length === 0) $controlGroup = $(buttonEl).closest('.form-group.has-suggestion');
            $controlGroup.find('.suggestion-field-loader').remove();
        },
        _showProcessingBanner: function() {
            var self = this;
            if ($('#scheming-processing-banner').length === 0) {
                var bannerHtml = '<div id="scheming-processing-banner" class="scheming-alert scheming-alert-info">' +
                                 self.options.processingMessage +
                                 '</div>';
                var $formContainer = $('.primary.span9').first();
                if ($formContainer.length === 0) $formContainer = $('form.dataset-form, form#dataset-edit').first();
                if ($formContainer.length === 0) $formContainer = $('main .container, #content .container').first();
                if ($formContainer.length) $formContainer.prepend(bannerHtml);
                else $('body').prepend(bannerHtml);
            }
        },
        _updateProcessingBanner: function(message, alertClass) {
            var $banner = $('#scheming-processing-banner');
            if ($banner.length === 0 && message) { // If banner doesn't exist, create it
                this._showProcessingBanner();
                $banner = $('#scheming-processing-banner');
            }
            if ($banner.length) {
                var timestamp = new Date().toLocaleTimeString();
                $banner.html(message + ' <span style="font-size:0.7em; opacity:0.7;">(as of ' + timestamp + ')</span>')
                       .removeClass('scheming-alert-info scheming-alert-warning scheming-alert-danger scheming-alert-success')
                       .addClass(alertClass || 'scheming-alert-info');
            }
        },
        _removeProcessingBanner: function() {
            $('#scheming-processing-banner').fadeOut(function() { $(this).remove(); });
        },

        _processDppButtonSuggestions: function(dppPackageSuggestions) {
            var self = this;
            if (!dppPackageSuggestions) {
                return;
            }
            $('button[data-module="scheming-suggestions"]').each(function() {
                var $buttonEl = $(this);
                var fieldName = $buttonEl.data('field-name');
                var fieldSchemaJson = $buttonEl.data('field-schema');
                if (!fieldSchemaJson) {
                    self._hideFieldLoadingIndicator($buttonEl);
                    $buttonEl.attr('title', self.options.noSuggestionTitle).show();
                    return;
                }
                var fieldSchema = typeof fieldSchemaJson === 'string' ? JSON.parse(fieldSchemaJson) : fieldSchemaJson;

                if (dppPackageSuggestions.hasOwnProperty(fieldName)) {
                    var suggestionValue = dppPackageSuggestions[fieldName];
                    
                    // Check if suggestion value is null/undefined/None string - disable button if so
                    if (suggestionValue === null || suggestionValue === undefined || suggestionValue === 'None' || suggestionValue === '') {
                        $buttonEl.addClass('suggestion-btn-disabled');
                        $buttonEl.attr('title', self.options.noSuggestionTitle);
                        $buttonEl.prop('disabled', true);
                        $buttonEl.show();
                        self._hideFieldLoadingIndicator($buttonEl);
                        return;
                    }
                    
                    var isErrorSuggestion = typeof suggestionValue === 'string' && suggestionValue.startsWith(self.options.errorPrefix);
                    var suggestionLabel = fieldSchema.suggestion_label || fieldSchema.label || 'Suggestion';
                    var suggestionFormula = fieldSchema.suggestion_formula || 'N/A'; 
                    var isSelect = fieldSchema.is_select;
                    var isValidSuggestion = dppPackageSuggestions[fieldName + '_is_valid'];
                    if (isValidSuggestion === undefined) {
                         isValidSuggestion = true;
                         if (isSelect && fieldSchema.choices && fieldSchema.choices.length > 0) {
                            isValidSuggestion = fieldSchema.choices.some(function(choice){ return String(choice.value) === String(suggestionValue); });
                         }
                    }

                    if (isErrorSuggestion) {
                        $buttonEl.addClass('suggestion-btn-error'); $buttonEl.attr('title', self.options.suggestionErrorTitle); suggestionLabel = 'Suggestion Error';
                    } else {
                        $buttonEl.removeClass('suggestion-btn-error'); $buttonEl.attr('title', self.options.suggestionReadyTitle);
                    }

                    if (!self._popoverDivs[fieldName]) {
                        var popoverId = 'custom-suggestion-popover-' + fieldName + '-' + Date.now();
                        self._popoverDivs[fieldName] = $('<div class="custom-suggestion-popover" id="' + popoverId + '" style="display: none;"></div>').appendTo('body');
                        self._attachBaseEventHandlers($buttonEl, self._popoverDivs[fieldName], fieldName);
                    }

                    self._populatePopoverContent($buttonEl, self._popoverDivs[fieldName], {
                        value: suggestionValue, label: suggestionLabel, formula: suggestionFormula,
                        is_select: isSelect, is_valid: isErrorSuggestion ? false : isValidSuggestion,
                        field_name: fieldName, is_error: isErrorSuggestion
                    });
                    $buttonEl.show();

                } else {
                    $buttonEl.attr('title', self.options.noSuggestionTitle).show();
                }
                self._hideFieldLoadingIndicator($buttonEl);
            });
        },

        _setFieldValue: function($target, value, fieldNameForLog) {
            var self = this;
            var fieldId = fieldNameForLog || $target.data('scheming-field-name') || $target.attr('name') || $target.attr('id');
            var success = false;

            if ($target.is('textarea, input[type="text"], input[type="number"], input[type="email"], input[type="url"], input[type="hidden"], input:not([type="button"], [type="submit"], [type="reset"], [type="image"], [type="file"], [type="radio"], [type="checkbox"])')) {
                var currentValue = $target.val();
                var newValueString = (value === null || value === undefined) ? "" : String(value);
                if (currentValue !== newValueString) {
                    $target.val(newValueString).trigger('change');
                }
                success = true;
            } else if ($target.is('select')) {
                var originalValueStr = (value === null || value === undefined) ? "" : String(value);
                var $optionToSelect = null;
                var $optionByValueExact = $target.find("option[value='" + originalValueStr.replace(/'/g, "&apos;") + "']");
                if ($optionByValueExact.length > 0) $optionToSelect = $optionByValueExact.first();
                else {
                    var foundLoose = false;
                    $target.find("option").each(function() {
                        if ($(this).val() == value) { $optionToSelect = $(this); foundLoose = true; return false; }
                    });
                    if (!foundLoose) {
                        var $optionByText = $target.find("option").filter(function() { return $(this).text() === originalValueStr; });
                        if ($optionByText.length > 0) $optionToSelect = $optionByText.first();
                    }
                }
                if ($optionToSelect) {
                    if ($target.val() !== $optionToSelect.val()) $target.val($optionToSelect.val()).trigger('change');
                    if (typeof $target.chosen === 'function') $target.trigger("chosen:updated");
                    if (typeof $target.select2 === 'function') $target.trigger('change.select2');
                    success = true;
                } else {
                    console.warn("SchemingSuggestions: Value '" + esc(value) + "' not valid for select:", fieldId);
                }
            }
            return success;
        },

        _updateLiveDatasetAndFormulaFields: function(datasetObject, dppSuggestionsObject) {
            var self = this;
            var updatedFieldsLog = [];

            if (!datasetObject || typeof datasetObject !== 'object') {
                console.warn("SchemingSuggestions: _updateLiveDatasetAndFormulaFields called with invalid datasetObject.");
                return;
            }

            var dppFieldName = 'dpp_suggestions';
            var $dppTextarea = $('#field-' + dppFieldName);
            if ($dppTextarea.length === 0) $dppTextarea = $('textarea[data-scheming-field-name="' + dppFieldName + '"], textarea[name="' + dppFieldName + '"]');
            if ($dppTextarea.length && dppSuggestionsObject && typeof dppSuggestionsObject === 'object') {
                try {
                    var prettyJson = JSON.stringify(dppSuggestionsObject, null, 2);
                    if (self._setFieldValue($dppTextarea, prettyJson, dppFieldName)) {
                        $dppTextarea.addClass('formula-auto-applied');
                        setTimeout(function() { $dppTextarea.removeClass('formula-auto-applied'); }, 1200);
                        updatedFieldsLog.push(dppFieldName + " (JSON content)");
                    }
                } catch (e) { console.error("SchemingSuggestions: Error handling '"+dppFieldName+"' field:", e); }
            }

            $('form.dataset-form [data-scheming-field-name][data-is-formula-field="true"]').each(function() {
                var $target = $(this);
                var fieldName = $target.data('scheming-field-name');

                if ($target.closest('.scheming-resource-fields, .resource-item, .repeating-template').length > 0) return; // Skip resource fields here

                if (datasetObject.hasOwnProperty(fieldName)) {
                    var newValue = datasetObject[fieldName];
                    var isError = typeof newValue === 'string' && newValue.startsWith(self.options.errorPrefix);
                    $target.removeClass('formula-auto-applied formula-apply-error');
                    if (!isError) {
                        if (self._setFieldValue($target, newValue, fieldName)) {
                            $target.addClass('formula-auto-applied');
                            setTimeout(function() { $target.removeClass('formula-auto-applied'); }, 1200);
                            updatedFieldsLog.push("Dataset Formula: " + fieldName);
                        } else $target.addClass('formula-apply-error');
                    } else {
                        self._setFieldValue($target, newValue, fieldName); $target.addClass('formula-apply-error');
                        updatedFieldsLog.push("Dataset Formula (error set): " + fieldName);
                    }
                } else {
                }
            });

            if (datasetObject.resources && Array.isArray(datasetObject.resources)) {
                datasetObject.resources.forEach(function(resource, resourceIndex) {
                    // Try to find the DOM element for this resource
                    var $resourceForm = null;
                    if (resource.id) {
                         $resourceForm = $('.resource-item input[name$="].id"][value="'+resource.id+'"]').closest('.resource-item');
                         if ($resourceForm.length === 0) { // Try another common selector
                             $resourceForm = $('.scheming-resource-fields input[name$="].id"][value="'+resource.id+'"]').closest('.scheming-resource-fields');
                         }
                    }
                    if (!$resourceForm || $resourceForm.length === 0) {
                        $resourceForm = $('.resource-item, .scheming-resource-fields').eq(resourceIndex);
                    }

                    if ($resourceForm && $resourceForm.length) {
                        $resourceForm.find('[data-scheming-field-name][data-is-formula-field="true"]').each(function() {
                            var $target = $(this);
                            var fieldName = $target.data('scheming-field-name');
                            if (resource.hasOwnProperty(fieldName)) {
                                var newValue = resource[fieldName];
                                var isError = typeof newValue === 'string' && newValue.startsWith(self.options.errorPrefix);
                                $target.removeClass('formula-auto-applied formula-apply-error');
                                if (!isError) {
                                    if (self._setFieldValue($target, newValue, fieldName)) {
                                        $target.addClass('formula-auto-applied');
                                        setTimeout(function() { $target.removeClass('formula-auto-applied'); }, 1200);
                                        updatedFieldsLog.push("Resource[" + (resource.id || resourceIndex) + "] Formula: " + fieldName);
                                    } else $target.addClass('formula-apply-error');
                                } else {
                                    self._setFieldValue($target, newValue, fieldName); $target.addClass('formula-apply-error');
                                    updatedFieldsLog.push("Resource[" + (resource.id || resourceIndex) + "] Formula (error set): " + fieldName);
                                }
                            }
                        });
                    } else {
                        // console.warn("SchemingSuggestions: Could not find DOM for resource index " + resourceIndex + " (ID: " + (resource.id || 'N/A') + ")");
                    }
                });
            }

            if (updatedFieldsLog.length > 0) console.log("SchemingSuggestions: _updateLiveDatasetAndFormulaFields updated:", updatedFieldsLog);
        },

        _pollForSuggestions: function() {
            var self = this;

            if (!globalState.datasetId) {
                if (globalState.pollAttempts === 0) {
                    globalState.pollAttempts++;
                    setTimeout(function() { self._pollForSuggestions(); }, self.options.pollingInterval + 1000);
                    return;
                }
                self._updateProcessingBanner(self.options.errorMessage + " (Dataset ID missing)", 'scheming-alert-danger');
                self._showAllButtonsAsNoSuggestion(); globalState.isPolling = false; return;
            }

            if (globalState.isPolling && globalState.pollAttempts > 0 && !this._isFirstPollerInstance) {
            }


            if (globalState.pollAttempts >= self.options.maxPollAttempts) {
                if (!$('#scheming-processing-banner').hasClass('scheming-alert-success') && !$('#scheming-processing-banner').hasClass('scheming-alert-danger')) {
                    self._updateProcessingBanner(self.options.timeoutMessage, 'scheming-alert-warning');
                }
                self._showAllButtonsAsNoSuggestion(); globalState.isPolling = false; return;
            }

            if (globalState.pollAttempts === 0) { // First actual poll attempt for a dataset ID
                 if (globalState.isPolling) { // Already polling from another instance, this one should not start another.
                      return;
                 }
                 globalState.isPolling = true;
                 this._isFirstPollerInstance = true; // This instance is managing the poll loop.
            } else if (!globalState.isPolling) { // Should not happen if logic is correct, but as a safeguard
                globalState.isPolling = true;
            }
            if (globalState.pollAttempts === 0 && $('#scheming-processing-banner').length === 0) {
                var self = this;
                // Quick check to see if we should show the banner
                $.ajax({
                    url: (ckan.SITE_ROOT || '') + '/api/3/action/package_show',
                    data: { id: globalState.datasetId, include_tracking: false },
                    dataType: 'json',
                    cache: false,
                    async: false, // Make synchronous just for this initial check
                    success: function(response) {
                        if (response.success && response.result && response.result.dpp_suggestions) {
                            var status = response.result.dpp_suggestions.STATUS;
                            if (!status || !self.options.terminalStatuses.includes(status.toUpperCase())) {
                                self._showProcessingBanner();
                            }
                        } else {
                            self._showProcessingBanner();
                        }
                    },
                    error: function() {
                        self._showProcessingBanner();
                    }
                });
            }
            
            $.ajax({
                url: (ckan.SITE_ROOT || '') + '/api/3/action/package_show',
                data: { id: globalState.datasetId, include_tracking: false },
                dataType: 'json',
                cache: false,
                success: function(response) {
                    globalState.pollAttempts++;

                    if (response.success && response.result) {
                        var datasetObject = response.result;
                        var dppSuggestionsData = datasetObject.dpp_suggestions; // This is the direct JSON object

                        if (dppSuggestionsData && dppSuggestionsData.package) {
                            self._processDppButtonSuggestions(dppSuggestionsData.package);
                        } else {
                            self._processDppButtonSuggestions(null);
                        }
                        var currentDppStatus = (dppSuggestionsData && dppSuggestionsData.STATUS) ? dppSuggestionsData.STATUS.toUpperCase() : null;

                        if (currentDppStatus === 'DONE') {
                            console.log("SchemingSuggestions: STATUS is DONE. Applying final updates to formula fields from main dataset object.");
                            self._updateLiveDatasetAndFormulaFields(datasetObject, dppSuggestionsData);
                            globalState.isInitialLoadWithExistingSuggestions = (globalState.pollAttempts === 1);
                        } else if (dppSuggestionsData) { // If dpp_suggestions exists, update its textarea
                            self._updateLiveDatasetAndFormulaFields(null, dppSuggestionsData); // Only update dpp_suggestions field
                        }


                        if (currentDppStatus) {
                            if (self.options.terminalStatuses.includes(currentDppStatus)) {
                                if (currentDppStatus === 'DONE') {
                                    if (!globalState.isInitialLoadWithExistingSuggestions) {
                                        self._updateProcessingBanner(self.options.statusDoneText, 'scheming-alert-success');
                                        setTimeout(function() { self._removeProcessingBanner(); }, 5000);  
                                    }
                                } else { // ERROR, FAILED
                                    if ($('#scheming-processing-banner').length > 0) {
                                        self._updateProcessingBanner(self.options.statusErrorText + esc(dppSuggestionsData.STATUS) + '</span>', 'scheming-alert-danger');
                                    }
                                }
                                globalState.isPolling = false; return;
                            } else { // Ongoing status
                                self._updateProcessingBanner(self.options.statusProcessingTextPrefix + esc(dppSuggestionsData.STATUS) + '</span>', 'scheming-alert-info');
                                setTimeout(function() { self._pollForSuggestions(); }, self.options.pollingInterval);
                            }
                        } else { // No STATUS in dpp_suggestions
                            console.warn("SchemingSuggestions: Poll " + globalState.pollAttempts + ": dpp_suggestions object has no STATUS field.");
                            if (globalState.pollAttempts < self.options.maxPollAttempts) {
                               setTimeout(function() { self._pollForSuggestions(); }, self.options.pollingInterval);
                            } else { // Max attempts reached with no status
                                if (!$('#scheming-processing-banner').hasClass('scheming-alert-success') && !$('#scheming-processing-banner').hasClass('scheming-alert-danger')) {
                                   self._updateProcessingBanner('<span><i class="fa fa-info-circle"></i> Processing status unclear. Max attempts reached.</span>', 'scheming-alert-warning');
                                }
                                globalState.isPolling = false;
                            }
                        }
                    } else { // API success:false or no result
                        console.error("SchemingSuggestions: Poll " + globalState.pollAttempts + ": API error/unexpected structure.", response);
                        if (globalState.pollAttempts < self.options.maxPollAttempts) {
                           setTimeout(function() { self._pollForSuggestions(); }, self.options.pollingInterval * 1.5);
                        } else {
                           self._updateProcessingBanner(self.options.errorMessage + " (API response error)", 'scheming-alert-danger');
                           self._showAllButtonsAsNoSuggestion(); globalState.isPolling = false;
                        }
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.error("SchemingSuggestions: Poll " + (globalState.pollAttempts +1) + ": AJAX Error:", textStatus, errorThrown, jqXHR.status, jqXHR.responseText);
                    globalState.pollAttempts++;
                    if (globalState.pollAttempts < self.options.maxPollAttempts) {
                        var nextPollDelay = self.options.pollingInterval * Math.pow(1.2, Math.min(globalState.pollAttempts, 7));
                        setTimeout(function() { self._pollForSuggestions(); }, nextPollDelay);
                    } else {
                        if (!$('#scheming-processing-banner').hasClass('scheming-alert-success')) { // Don't overwrite if it somehow succeeded then errored
                           self._updateProcessingBanner(self.options.errorMessage + " (API connection error)", 'scheming-alert-danger');
                        }
                        self._showAllButtonsAsNoSuggestion(); globalState.isPolling = false;
                    }
                }
            });
        },
                _populatePopoverContent: function($buttonEl, $popoverDiv, suggestionData) {
            var self = this;
            
            // Check if this is a multiple date column suggestion by trying to parse as JSON
            var multipleDateOptions = null;
            try {
                var parsed = JSON.parse(suggestionData.value);
                if (Array.isArray(parsed) && parsed.length > 1 && parsed[0].field_name && parsed[0].date) {
                    multipleDateOptions = parsed;
                }
            } catch (e) {
                // Not JSON or not the expected format, proceed normally
            }
            
            if (multipleDateOptions) {
                // Generate radio button interface for multiple date columns
                var radioOptionsHtml = multipleDateOptions.map(function(option, index) {
                    var radioId = 'date-option-' + suggestionData.field_name + '-' + index;
                    return `
                        <div class='date-option-row'>
                            <input type='radio' id='${radioId}' name='date-options-${suggestionData.field_name}' 
                                   value='${esc(option.date)}' data-field='${esc(option.field_name)}' 
                                   ${index === 0 ? 'checked' : ''}>
                            <label for='${radioId}'>
                                <strong>${esc(option.field_name)}:</strong> ${esc(option.date)}
                            </label>
                        </div>`;
                }).join('');
                
                var popoverContentHtml = `
                    <div class='suggestion-popover-content'>
                        <strong>${esc(suggestionData.label)} - Multiple Date Columns Found</strong>
                        <div class='multiple-date-options'>
                            <p>Select the date column to use for this field:</p>
                            ${radioOptionsHtml}
                        </div>
                        ${!suggestionData.is_error ? `
                            <div class='formula-toggle'>
                                <button class='formula-toggle-btn' type='button'>
                                    <span class='formula-toggle-icon'>&#9660;</span>
                                    <span class='formula-toggle-text'>Show formula</span>
                                </button>
                            </div>
                            <div class='suggestion-formula' style='display:none;'>
                                <div class='formula-header'>
                                    <span>Formula:</span>
                                    <button class='copy-formula-btn' type='button' data-formula='${esc(suggestionData.formula)}' title="Copy formula">
                                        <svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24'><path fill='currentColor' d='M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z'/></svg>
                                    </button>
                                </div>
                                <code>${esc(suggestionData.formula)}</code>
                            </div>` : ''}
                        <button class='suggestion-apply-btn'
                                data-target='field-${suggestionData.field_name}'
                                data-multiple-options='true'>
                            Apply Selected Date
                        </button>
                    </div>`;
            } else {
                // Original single value suggestion interface
                var popoverContentHtml = `
                    <div class='suggestion-popover-content ${suggestionData.is_error ? "suggestion-popover-error" : ""}'>
                        <strong>${esc(suggestionData.label)}</strong>
                        ${suggestionData.is_error ? `
                            <div class='suggestion-error-text'>
                                <svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>
                                    <circle cx='12' cy='12' r='10'></circle><line x1='12' y1='8' x2='12' y2='12'></line><line x1='12' y1='16' x2='12.01' y2='16'></line>
                                </svg>
                                <span>The suggestion could not be generated correctly:</span>
                            </div>` : ''}
                        ${suggestionData.is_select && !suggestionData.is_valid && !suggestionData.is_error ? `
                            <div class='suggestion-warning'>
                                <svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>
                                    <circle cx='12' cy='12' r='10'></circle><line x1='12' y1='8' x2='12' y2='12'></line><line x1='12' y1='16' x2='12.01' y2='16'></line>
                                </svg>
                                <span>This value is not a valid choice for this field.</span>
                            </div>` : ''}
                        <div class='suggestion-value ${suggestionData.is_error ? "suggestion-value-error" : ""}'>${esc(suggestionData.value)}</div>
                        ${!suggestionData.is_error ? `
                            <div class='formula-toggle'>
                                <button class='formula-toggle-btn' type='button'>
                                    <span class='formula-toggle-icon'>&#9660;</span>
                                    <span class='formula-toggle-text'>Show formula</span>
                                </button>
                            </div>
                            <div class='suggestion-formula' style='display:none;'>
                                <div class='formula-header'>
                                    <span>Formula:</span>
                                    <button class='copy-formula-btn' type='button' data-formula='${esc(suggestionData.formula)}' title="Copy formula">
                                        <svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24'><path fill='currentColor' d='M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z'/></svg>
                                    </button>
                                </div>
                                <code>${esc(suggestionData.formula)}</code>
                            </div>` : ''}
                        <button class='suggestion-apply-btn ${(!suggestionData.is_valid || suggestionData.is_error) ? "suggestion-apply-btn-disabled" : ""}'
                                data-target='field-${suggestionData.field_name}'
                                data-value='${String(suggestionData.value).replace(/'/g, "&apos;").replace(/"/g, "&quot;")}'
                                data-is-select='${suggestionData.is_select}'
                                data-is-valid='${suggestionData.is_valid}'>
                            ${suggestionData.is_error ? 'Error in Suggestion' : 'Apply suggestion'}
                        </button>
                    </div>`;
            }
            
            $popoverDiv.html(popoverContentHtml);
            this._attachActionHandlers($popoverDiv, suggestionData.field_name);
        },
        _attachBaseEventHandlers: function(el, $popoverDiv, fieldName) {
            var self = this;
            $(el).on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                
                // Prevent clicks on disabled buttons
                if ($(el).hasClass('suggestion-btn-disabled') || $(el).prop('disabled')) {
                    return;
                }
                
                if ($popoverDiv.is(':empty') && !$popoverDiv.html().trim()) { return; } // Check if truly empty
                $('.custom-suggestion-popover').not($popoverDiv).hide();
                var buttonPos = $(el).offset();
                var windowWidth = $(window).width();
                var popoverCalculatedWidth = Math.min(380, windowWidth - 40);
                var leftPos = buttonPos.left;
                if (buttonPos.left + popoverCalculatedWidth > windowWidth - 20) {
                    leftPos = Math.max(20, windowWidth - popoverCalculatedWidth - 20);
                }
                var topPos = buttonPos.top + $(el).outerHeight() + 10;
                if (topPos < $(window).scrollTop()){ topPos = $(window).scrollTop() + 10; }
                $popoverDiv.css({ position: 'absolute', top: topPos, left: leftPos, width: popoverCalculatedWidth + 'px', zIndex: 1050 }).toggle();
            });
        },
        _attachActionHandlers: function($popoverDiv, fieldName) {
            var self = this;
            $(document).off('click.schemingSuggestionsGlobal.' + fieldName).on('click.schemingSuggestionsGlobal.' + fieldName, function(e) {
                if (!$(e.target).closest($popoverDiv).length && !$(e.target).closest('button[data-field-name="'+fieldName+'"]').length) {
                    $popoverDiv.hide();
                }
            });
            $popoverDiv.off('click.formulaToggle').on('click.formulaToggle', '.formula-toggle-btn', function(e) {
                e.preventDefault(); e.stopPropagation();
                var $formulaSection = $(this).closest('.suggestion-popover-content').find('.suggestion-formula');
                var $toggleIcon = $(this).find('.formula-toggle-icon');
                var $toggleText = $(this).find('.formula-toggle-text');
                $formulaSection.slideToggle(200, function() {
                    if ($formulaSection.is(':visible')) {
                        $toggleIcon.html('&#9650;'); $toggleText.text('Hide formula'); $(this).closest('.formula-toggle').addClass('formula-toggle-active');
                    } else {
                        $toggleIcon.html('&#9660;'); $toggleText.text('Show formula'); $(this).closest('.formula-toggle').removeClass('formula-toggle-active');
                    }
                });
            });
            $popoverDiv.off('click.copyFormula').on('click.copyFormula', '.copy-formula-btn', function(e) {
                e.preventDefault(); e.stopPropagation();
                var formula = $(this).data('formula');
                var $copyBtn = $(this);
                navigator.clipboard.writeText(formula).then(function() {
                    var $iconContainer = $copyBtn.find('svg').parent();
                    var originalIcon = $iconContainer.html();
                    $iconContainer.html('<svg width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41L9 16.17z"/></svg>');
                    $copyBtn.addClass('copy-success');
                    setTimeout(function() { $copyBtn.removeClass('copy-success'); $iconContainer.html(originalIcon); }, 2000);
                }).catch(function(err) {
                    console.error('Could not copy formula: ', err);
                    self._showTemporaryMessage(null, "Could not copy formula.", 'suggestion-warning-message', '#e67e22');
                });
            });
            $popoverDiv.off('click.applySugg').on('click.applySugg', '.suggestion-apply-btn', function(e) {
                e.preventDefault(); e.stopPropagation();
                if ($(this).hasClass('suggestion-apply-btn-disabled')) return;
                var targetId = $(this).data('target');
                var isMultipleOptions = $(this).data('multiple-options');
                var $target = $('#' + targetId);

                if (!$target.length) { console.error("Scheming Popover Apply: Target not found:", targetId); return; }

                var applySuccess = false;
                var suggestionValue;
                
                if (isMultipleOptions) {
                    // Handle multiple date column options - get selected radio button value
                    var $selectedRadio = $popoverDiv.find('input[name="date-options-' + fieldName + '"]:checked');
                    if ($selectedRadio.length) {
                        suggestionValue = $selectedRadio.val();
                        var selectedField = $selectedRadio.data('field');
                        console.log("Selected date field:", selectedField, "with value:", suggestionValue);
                    } else {
                        self._showTemporaryMessage($target, "Please select a date column option.", 'suggestion-warning-message', '#e67e22');
                        return;
                    }
                } else {
                    // Handle single suggestion value
                    suggestionValue = $(this).data('value');
                    var isValid = $(this).data('is-valid') !== false;
                }

                if (self._setFieldValue($target, suggestionValue, targetId.substring(6))) {
                    applySuccess = true;
                } else if (!$target.is('select')) {
                    self._showTemporaryMessage($target, "Could not apply suggestion to this field type.", 'suggestion-warning-message', '#e67e22');
                }

                if (applySuccess) {
                    $target.addClass('suggestion-applied');
                    setTimeout(function() { $target.removeClass('suggestion-applied'); }, 1200);
                    self._showTemporaryMessage($target, "Suggestion applied!", 'suggestion-success-message', 'rgba(40, 167, 69, 0.95)');
                } else if (!isMultipleOptions && $target.is('select') && !isValid) {
                     self._showTemporaryMessage($target, "The suggested value is not a valid option.", 'suggestion-warning-message', '#e67e22');
                     $target.addClass('suggestion-invalid');
                     setTimeout(function() { $target.removeClass('suggestion-invalid'); }, 3000);
                }
                $popoverDiv.hide();
            });
        },
        _showTemporaryMessage: function($targetElement, message, cssClass, bgColor) {
            $('.scheming-temp-message').remove();
            var $msg = $('<div></div>').addClass('scheming-temp-message ' + cssClass).text(message).css({
                position: 'fixed', bottom: '20px', left: '50%', transform: 'translateX(-50%)',
                backgroundColor: bgColor || '#333', color: 'white', padding: '10px 20px',
                borderRadius: '6px', fontSize: '14px', fontWeight: '500', zIndex: 2000,
                opacity: 0, boxShadow: '0 5px 15px rgba(0,0,0,0.2)'
            });
            $('body').append($msg);
            $msg.animate({opacity: 1, bottom: '30px'}, 300, 'swing')
                .delay(2500)
                .animate({opacity: 0, bottom: '20px'}, 300, 'swing', function() { $(this).remove(); });
        },
        finalize: function() {
            var fieldName = this.el && $(this.el).data('field-name');
            if (fieldName) {
                $(document).off('click.schemingSuggestionsGlobal.' + fieldName);
                if (this._popoverDivs[fieldName]) {
                    this._popoverDivs[fieldName].remove();
                    delete this._popoverDivs[fieldName];
                }
            }
        }
    };
});