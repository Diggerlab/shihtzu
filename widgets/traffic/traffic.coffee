class Dashing.Traffic extends Dashing.Widget

  @accessor 'current_in', ->
    return @get('displayedValue_in') if @get('displayedValue_in')
    points_in = @get('points_in')
    if points_in
      points_in[points_in.length - 1].y


  @accessor 'current_out', ->
    return @get('displayedValue_out') if @get('displayedValue_out')
    points_out = @get('points_out')
    if points_out
      points_out[points_out.length - 1].y


  ready: ->
    container = $(@node).parent()
    # Gross hacks. Let's fix this.
    width = (Dashing.widget_base_dimensions[0] * container.data("sizex")) + Dashing.widget_margins[0] * 2 * (container.data("sizex") - 1)
    height = (Dashing.widget_base_dimensions[1] * container.data("sizey")) + Dashing.widget_margins[1] * 2 * (container.data("sizey") - 1)
    @traffic = new Rickshaw.Graph(
      element: @node
      width: width
      height: height
      renderer: 'bar'
      stroke: true
      series: [
        {
        color: "#fff",
        data: [{x:0, y:0}]
        },
        {
        color: "read",
        data: [{x:0, y:0}]
        }
      ]
    )

    @traffic.series[0].data = @get('points_in') if @get('points_in')
    @traffic.series[1].data = @get('points_out') if @get('points_out')

    x_axis = new Rickshaw.Graph.Axis.Time(graph: @traffic)

    y_axis = new Rickshaw.Graph.Axis.Y(graph: @traffic, tickFormat: Rickshaw.Fixtures.Number.formatKMBT)

    @traffic.renderer.unstack = true;
    @traffic.render()

  onData: (data) ->
    if @traffic 
      @traffic.series[0].data = data.points_in
      @traffic.series[1].data = data.points_out
      @traffic.render()
